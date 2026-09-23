# 日常操作

初回設定は[はじめる](getting-started.md)、設定値の変更は[設定](configuration.md)を参照してください。Docker版のコンテナ起動・停止とファイルの取り出しは[Dockerで使う](docker.md)にまとめています。

## 対話する

管理画面で **Start dialogue server** → **Running** → **Open dialogue** の順に進みます。

- **Shift+Enter** または **Send** で送信し、**Enter** で改行します。
- 発話の追加・生成途中の更新に合わせて、履歴は最新の本文へスクロールします。過去を読んでいても、新しい更新があれば最新へ戻ります。
- `Generating response (attempt 1)` は今回の応答の初回生成中という意味です。正常に応答が確定すると消えます。
- 音声が設定されている場合は **Voice output** をONにできます。ページを読み込み直すとOFFに戻ります。

ブラウザーを閉じてもサーバーは止まりません。設定で有効になっているバックグラウンド処理も、対話サーバーの稼働中は動きます。

30分の間隔によるセッションの区切りは維持されますが、新しいセッションの初期には、通常の発話でも直前1往復（未回答ならユーザー発話1件）が文脈へ引き継がれます。現在の発話を含む7イベントの枠内で扱い、新しい会話が増えると外れます。消去済みの本文や別の参加者との会話は引き継ぎません。全過去履歴や長期記憶を自動で参照する機能ではなく、記憶のRecallは[Memory policy v5](configuration.md#記憶とself-talk)で別途有効にします。

## 停止と再起動

管理画面から起動した対話は **Stop dialogue server** で停止し、**Stopped** になるまで待ちます。管理画面自体は引き続き使えます。再開は **Start dialogue server** です。

管理サーバーも終了する場合は、起動したターミナルで **Ctrl+C** を押し、終了を待ちます。管理サーバーの終了時には、その管理サーバーが起動した対話も停止します。

`mahoroba serve` で別に起動した対話は、管理画面のStopでは止まりません。起動元のターミナルで終了してください。アプリ更新後はサーバープロセスを再起動し、画面を読み込み直します。ブラウザーの更新だけでは実行中のコードは更新されません。

設定変更時にどこまで再起動するかは[設定の反映](configuration.md#設定の反映)を参照してください。

## Residentを切り替える・利用を終える

対話を停止してから、**Resident → List residents** でIDを確認し、**Select resident** でactiveなResidentを選択します。その後に対話を起動します。

**Archive resident** はそのResidentの活動を終了する操作で、現行UI/CLIではactiveへ戻せません。一時停止なら対話をStopするか、別のactiveなResidentを選びます。Archiveは履歴本文の消去ではありません。

内容を消去する場合は **Blobs and erasure → Plan resident erasure** で計画を作り、判断を求められた場合に **Review erasure plan** を行い、**Apply erasure plan** で適用します。計画と判断を確認し、必要なダイジェストを指定する別の手順です。詳しくは[操作一覧と消去の確認](web-administration.md#available-operations)を参照してください。

## 管理操作を実行する

操作を選び、必要な入力と確認を行って実行します。**Stop dialogue first** と表示される操作は、対話停止後に行います。

Residentの一覧・詳細、Diagnostics、バックアップ作成、JSONL exportも対話停止が必要です。管理画面自体は開いたままにできます。既存バックアップの **Verify backup** と、新しい保存先への **Restore backup** はこの停止要求の対象外ですが、対象パスの排他・重複確認は行われます。

claimの可視性や状態を変える場合も、対話を停止し、**Memory → List memory claims / Show memory claim** で対象を確認してから **Change claim visibility / Change claim status** を使います。入力するのは文章ではなく **Resident ID** と **Claim ID**（ULID）です。置換先を要求する状態変更では、そのClaim IDも指定します。

結果には終了コードと出力が表示されます。`exit_code = 0` が成功です。失敗時は出力の理由や残作業を確認してから再試行してください。利用できる41操作は[Web管理ガイド](web-administration.md#available-operations)にまとまっています。

## バックアップと復元

1. 対話を停止し、**Backup and export → Create backup** を選びます。
2. **Output absolute path** に、まだ存在しないバックアップ先ディレクトリの絶対パスを指定して実行します。
3. **Verify backup** にそのパスを指定し、検証結果を確認します。

応答直後の停止では、派生データの `content_references` が最後の更新に追いつかず、作成が `source_unavailable` で拒否されることがあります。停止したまま **Inspection and maintenance → Diagnostics / Projection status** を確認し、整合性に問題がなく、この派生データの遅延だけがある場合は、該当する各Residentについて **Rebuild projections** の **Rebuild all** を実行してから作成を再試行します。CLIでは `mahoroba projection rebuild --resident <Resident-ID> --all` です。停止後に待つだけでは再構築されません。他の整合性エラーがある場合は、その原因の確認を優先してください。

**Restore backup** は既存のデータを上書きせず、新しい **Target data directory absolute path** へ復元します。復元先を使うときは管理サーバーを終了し、その保存先を `--data-dir` に指定して起動し直します。

パスはサーバーが動いているPCのファイルシステムを指します。ブラウザーの **Download result** は表示中の結果テキストの保存で、バックアップ本体のダウンロードではありません。`Export JSONL` は別の出力機能で、Restore用のバックアップとは区別してください。

## データの保存と削除

既定の保存先は[設定と保存先](configuration.md#設定ファイルとデータの保存先)を参照してください。データディレクトリには主に `mahoroba.db` と `blobs/` があり、全Residentの履歴・記憶・設定されたPersonaなどを保持します。

データディレクトリ全体の削除は、特定のResidentだけを消す操作ではありません。全Residentのローカルデータが失われます。また、別の場所にある設定ファイル、実行ファイル、環境変数のAPIキー、Tailscale設定、外部のバックアップは残るため、完全アンインストールにもなりません。稼働中のデータディレクトリを削除しないでください。
