(() => {
  "use strict";

  const MAX_AUDIO_BYTES = 8 * 1024 * 1024;
  const MAX_AUDIO_BASE64 = Math.ceil(MAX_AUDIO_BYTES / 3) * 4;
  const MAX_AUDIO_QUEUE = 8;
  const body = document.body;
  const form = document.getElementById("message-form");
  const input = document.getElementById("message");
  const button = form.querySelector("button");
  const conversation = document.getElementById("conversation");
  const dialogue = document.getElementById("dialogue");
  const provisional = document.getElementById("provisional");
  const notice = document.getElementById("notice");
  const connection = document.getElementById("connection-state");
  const audioToggle = document.getElementById("voice-output");
  const residentID = body.dataset.residentId;
  let provisionalRun = "";
  let generatingNoticeRun = "";
  let events = null;
  let audioQueue = [];
  let activeAudio = null;
  let activeAudioURL = "";
  let composing = false;
  let scrollFrame = null;

  const followLatest = () => {
    if (scrollFrame !== null) return;
    scrollFrame = requestAnimationFrame(() => {
      scrollFrame = null;
      conversation.scrollTop = conversation.scrollHeight;
    });
  };

  // Keep both committed history and the growing response above the composer.
  // Resizing the viewport, composer, or text must also reveal the latest line.
  const conversationResize = new ResizeObserver(followLatest);
  [conversation, dialogue, provisional].forEach((element) => conversationResize.observe(element));
  followLatest();

  const showNotice = (message, generatingRun = "") => {
    generatingNoticeRun = message ? generatingRun : "";
    notice.textContent = message;
    notice.classList.toggle("hidden", !message);
  };

  const clearEmptyState = () => {
    const empty = dialogue.querySelector(".empty-state");
    if (empty) empty.remove();
  };

  const appendCommitted = (event) => {
    if (!["user_message", "resident_message", "outbound_initiative"].includes(event.event_type)) return;
    if (!event.event_id || dialogue.querySelector(`[data-event-id="${CSS.escape(event.event_id)}"]`)) return;
    clearEmptyState();
    const article = document.createElement("article");
    const role = event.event_type === "user_message" ? "user" : "resident";
    article.className = `message message-${role}`;
    article.dataset.eventId = event.event_id;

    const header = document.createElement("header");
    const label = document.createElement("span");
    label.textContent = role === "user" ? "You" : document.querySelector("h1").textContent;
    header.appendChild(label);

    const content = document.createElement("p");
    content.textContent = event.text || "";
    article.append(header, content);
    dialogue.appendChild(article);
    followLatest();
  };

  const releaseActiveAudio = () => {
    if (activeAudioURL) URL.revokeObjectURL(activeAudioURL);
    activeAudioURL = "";
    activeAudio = null;
  };

  const stopAudio = () => {
    audioQueue = [];
    if (activeAudio) {
      activeAudio.pause();
      activeAudio.removeAttribute("src");
    }
    releaseActiveAudio();
  };

  const decodeWAV = (event) => {
    if (event.mime_type !== "audio/wav" || typeof event.audio_base64 !== "string" ||
        !event.audio_base64 || event.audio_base64.length > MAX_AUDIO_BASE64) return null;
    let binary;
    try {
      binary = atob(event.audio_base64);
    } catch (_) {
      return null;
    }
    if (binary.length < 12 || binary.length > MAX_AUDIO_BYTES ||
        binary.slice(0, 4) !== "RIFF" || binary.slice(8, 12) !== "WAVE") return null;
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    return bytes;
  };

  const playNextAudio = () => {
    if (activeAudio || !audioToggle || !audioToggle.checked || audioQueue.length === 0) return;
    const bytes = audioQueue.shift();
    activeAudioURL = URL.createObjectURL(new Blob([bytes], { type: "audio/wav" }));
    const playerURL = activeAudioURL;
    const player = new Audio(playerURL);
    let finished = false;
    activeAudio = player;
    const finish = () => {
      if (finished) return;
      finished = true;
      URL.revokeObjectURL(playerURL);
      if (activeAudio === player) {
        activeAudio = null;
        activeAudioURL = "";
        playNextAudio();
      }
    };
    player.addEventListener("ended", finish, { once: true });
    player.addEventListener("error", finish, { once: true });
    player.play().catch(() => {
      finish();
      showNotice("Voice playback was blocked by the browser.");
    });
  };

  const enqueueAudio = (event) => {
    if (!audioToggle || !audioToggle.checked || event.resident_id !== residentID) return;
    const bytes = decodeWAV(event);
    if (!bytes) return;
    if (audioQueue.length >= MAX_AUDIO_QUEUE) audioQueue.shift();
    audioQueue.push(bytes);
    playNextAudio();
  };

  const connectEvents = () => {
    if (events) events.close();
    const audioEnabled = Boolean(audioToggle && audioToggle.checked);
    events = new EventSource(audioEnabled ? "/events?audio=1" : "/events");
    events.onopen = () => {
      connection.textContent = "Connected";
      showNotice("");
    };
    events.onerror = (event) => {
      if (!event.data) connection.textContent = "Reconnecting...";
    };
    events.addEventListener("provisional", (message) => {
      const event = JSON.parse(message.data);
      if (event.resident_id !== residentID) return;
      const sameRun = provisionalRun === event.generation_run_id;
      provisionalRun = event.generation_run_id;
      const output = provisional.querySelector("p");
      output.textContent = sameRun ? output.textContent + (event.text || "") : (event.text || "");
      provisional.classList.remove("hidden");
      followLatest();
    });
    events.addEventListener("committed", (message) => {
      const event = JSON.parse(message.data);
      if (event.resident_id !== residentID) return;
      appendCommitted(event);
      if (event.event_type === "resident_message" && generatingNoticeRun &&
          event.generation_run_id === generatingNoticeRun) {
        showNotice("");
      }
      if (!event.generation_run_id || event.generation_run_id === provisionalRun) {
        provisional.classList.add("hidden");
        provisional.querySelector("p").textContent = "";
        provisionalRun = "";
      }
    });
    events.addEventListener("audio", (message) => enqueueAudio(JSON.parse(message.data)));
    events.addEventListener("error", (message) => {
      if (!message.data) return;
      const event = JSON.parse(message.data);
      if (event.resident_id !== residentID) return;
      provisional.classList.add("hidden");
      provisionalRun = "";
      showNotice(event.message || "The resident could not complete this response.");
    });
    events.addEventListener("status", (message) => {
      const event = JSON.parse(message.data);
      if (event.resident_id && event.resident_id !== residentID) return;
      if (event.status === "retry_pending") {
        provisional.classList.add("hidden");
        provisional.querySelector("p").textContent = "";
        provisionalRun = "";
      }
      showNotice(event.message || "", event.status === "generating" ? (event.generation_run_id || "") : "");
    });
  };

  if (body.dataset.ready === "true") {
    if (audioToggle) {
      // Browser form-state restoration must not opt a new page load into paid
      // or privacy-sensitive synthesis. Every load starts explicitly off.
      audioToggle.checked = false;
      audioToggle.addEventListener("change", () => {
        if (!audioToggle.checked) stopAudio();
        connectEvents();
      });
    }
    connectEvents();
  } else {
    connection.textContent = "Not ready";
  }

  input.addEventListener("compositionstart", () => { composing = true; });
  input.addEventListener("compositionend", () => { composing = false; });
  input.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" || !event.shiftKey || composing || event.isComposing || event.keyCode === 229) return;
    event.preventDefault();
    if (!event.repeat && !input.disabled && !button.disabled) form.requestSubmit();
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (input.disabled || button.disabled || !input.value.trim()) return;
    button.disabled = true;
    try {
      const response = await fetch(form.action, {
        method: "POST",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
        },
        body: new URLSearchParams({ message: input.value })
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || "Message was not accepted.");
      input.value = "";
      input.focus({ preventScroll: true });
    } catch (error) {
      showNotice(error.message || "Message was not accepted.");
    } finally {
      button.disabled = false;
    }
  });
})();
