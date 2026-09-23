# 設定

設定値の一覧は [config.example.toml](../../config.example.toml) にあります。使用中のTOMLを編集して反映します。管理画面にはTOML全体を編集する機能はありません。

Docker版の設定ファイル・保存先・変更の反映方法は[Dockerで使う](docker.md)を参照してください。このページの起動コマンドと既定の保存先はローカル実行版のものです。

## 設定ファイルとデータの保存先

`--config` を指定すると、そのTOMLを読みます。省略時は次の場所です。

| OS | 既定の設定ファイル | 既定のデータディレクトリ |
| --- | --- | --- |
| Windows | `%APPDATA%\Mahoroba\config.toml` | `%LOCALAPPDATA%\Mahoroba` |
| Linux | `$XDG_CONFIG_HOME/Mahoroba/config.toml`、未設定なら `~/.config/Mahoroba/config.toml` | `$XDG_DATA_HOME/mahoroba`、未設定なら `~/.local/share/mahoroba` |

通常の設定の優先順位は **CLI引数 → 環境変数 → TOML → 組み込み既定値** です。データ保存先は `--data-dir`、`MAHOROBA_DATA_DIR`、TOMLの `data_dir` で指定でき、指定する値には絶対パスを使います。

管理画面上部の **Data directory** で対象を確認できます。別の保存先を開くと、元のResidentが消えたのではなく、別のデータを見ている場合があります。

## 生成サービスとAPIキー

`generation.provider` は `chat-completions` を使います。`generation.base_url` と `generation.model` は対話起動に必要です。アプリがベースURLの末尾に `/chat/completions` を追加するので、モデル一覧やチャット画面のURLは指定しません。

通常の接続先にはHTTPSを使います。同じ端末の生成サービスへHTTPで接続する場合は、`http://127.0.0.1:8080/v1` のように数値のloopback IPを指定してください。`http://localhost:8080/v1` は、名前解決後にloopbackとなる場合でも許可されません。外部サービスには、そのサービスが案内する `https://` のAPIベースURLを設定します。

Dockerからホスト上の生成サービスへのHTTP接続には、別途 `MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP=true` と、正確な `host.docker.internal`、明示したポート番号が必要です。例は `http://host.docker.internal:8080/v1` です。この許可は通常のローカル版では既定でOFF、同梱Docker構成ではONです。他のホスト名やTTSへの許可ではありません。詳しくは[Dockerの接続設定](docker.md#1-生成サービスとポートを設定する)を参照してください。

APIの名前がChat Completions互換でも、モデルやサービスのchat templateによって受け付けるメッセージ列は異なります。本アプリは複数の `system` メッセージや、`user` / `assistant` が厳密に交互ではないメッセージ列を送る場合があります。`system` が先頭の1件だけ、またはroleの厳密な交互配置が必要なバックエンドとの互換性は現在保証していません。メッセージを結合・並べ替えする互換モードもありません。接続先の制約に合う生成サービスとモデル設定を使ってください。

APIキーは `MAHOROBA_PROVIDER_API_KEY` で渡します。設定方法は[初回設定](getting-started.md#2-設定を用意する)を参照してください。TOMLへの秘密値の記載はサポートしていません。

Linuxでは制約付きの秘密ファイルを指す `MAHOROBA_PROVIDER_API_KEY_FILE` も使えます。Windowsでは秘密ファイル方式は未対応です。直接値とファイル指定は併用できず、空の秘密値もエラーになります。

## 設定の反映

| 変更内容 | 反映方法 |
| --- | --- |
| 使用中TOMLの生成設定・self-talkなど | 対話をStop → Start。起動時に再読み込みます。 |
| `--config` / `--data-dir` の対象、ポート、remote許可フラグ | 管理サーバーを終了し、指定を変えて起動し直します。 |
| APIキーなどの起動環境変数 | 新しい値を持つシェルから管理サーバーを起動し直します。 |
| アプリ本体の更新 | サーバー終了 → 更新・ビルド → 起動 → ブラウザー再読み込み。 |

管理サーバーの終了方法は[停止と再起動](operations.md#停止と再起動)を参照してください。

## 記憶とself-talk

初期の `memory-policy-v1` ではRecallが無効ですが、通常の対話はできます。質問に応じた記憶のRecallを使う場合は、対話停止後に **Memory → Activate memory policy v5** を実行します。既存のResidentを自動でv5へ移行することはありません。

- **Resident ID**：対象ResidentのID。
- **Current memory policy**：現在の版を指定。**Autonomy → Autonomy status** の `active_memory_policy` で確認できます。
- v1から移行する場合は **Acknowledge enabling Recall** と **Acknowledge mandatory self-talk extraction** の両方を確認。
- v2からは後者の確認が必要。v3／v4からはこの2つの追加確認は不要です。

v5は現在の質問と記憶の文章の文字の重なりを使って候補を選びます。文字の一致がない記憶は選択されず、過去に繰り返しRecallされたこと自体は順位を押し上げません。同点では新しいULIDの記憶を優先します。意味の近さを完全に理解する検索ではないため、言い換えなどを拾えない場合があります。`confidence` は証拠量と支持・反証の重みから算出するスコアで、内容が真実である確率ではありません。

CLIでは、たとえばv4から `mahoroba admin memory policy activate-v5 --resident <Resident-ID> --from memory-policy-v4` で移行します。v5での再試行には `--from memory-policy-v5` を指定します。移行後に旧版へ戻す操作はありません。

既存のv4は引き続き対話に使用できます。`Activate memory policy v0` と `Activate autonomy memory policy` は、既にv2／v3になっている場合の互換再試行用です。今回のRecall改善を使う場合はv5を明示的に適用してください。

**self-talkの記憶抽出の承認と、self-talk生成のONは別です。** 生成を有効にするには、使用中TOMLの既存セクションを変更し、対話を再起動します。

```toml
[autonomy.self_talk]
enabled = true
```

対象は選択中かつactiveのResidentです。v4またはv5適用後に一度ユーザーメッセージを送り、待機します。既定は30分間隔、静穏時間23:00〜07:00、1時間2回・1日12回・連続10回までです。時刻は `timezone` に従い、送信直後の会話処理中などは抑止されます。

状態は **Autonomy status** の `features.self_talk` にある `configured`、`effective`、`eligible`、`blocking_reason`、`next_eligible_at` で確認できます。サーバーの **Running** 状態も別に確認してください。

self-talkは生成サービスを呼ぶ内部の活動で、対話画面には表示されません。画面に自発的に話しかける機能は別の `[autonomy.initiative]` です。どちらも既定はOFFです。self-talkだけを止めるには `enabled = false` に戻して対話を再起動します。

## 音声出力

`[autonomy.tts]` の設定と、対話画面の **Voice output** は別です。TTSは既定で無効です。

OpenAI Speech方式ではURL・モデル・voiceと `MAHOROBA_TTS_API_KEY`、AivisSpeech方式では別途動くエンジンと有効な `style_id` が必要です。TTS無効時やAivisSpeech使用時には、OpenAI Speech用の秘密環境変数を設定しないでください。設定項目は[設定例](../../config.example.toml)を参照してください。

## Tailscaleから対話する

管理画面はローカルのまま、対話画面だけをtailnetから利用できます。両リスナーをloopbackのままにし、管理起動時の `--dialogue-allow-remote` とTailscale Serveの対話ポートへの転送を組み合わせます。

具体的なコマンドと解除方法は[Web管理ガイドのTailscale手順](web-administration.md#access-dialogue-through-your-tailscale-network)を参照してください。Tailscale Serveの転送先は管理の8788ではなく、対話の8787です。remote許可はTOMLや管理画面からは変更できません。
