# 困ったとき

まず管理画面の対話状態・エラー文、操作結果の終了コードを確認します。**Running** は起動が完了した状態です。現在の準備状態は **Inspection and maintenance → Check dialogue server health** でも確認できます。

## 対話サーバーが起動しない

| 表示・症状 | 確認すること |
| --- | --- |
| `generation.base_url is required for serve` / `generation.model is required for serve` | [生成設定](configuration.md#生成サービスとapiキー)を埋め、使用中の設定ファイルを確認します。 |
| active／selectedなResidentがない | Bootstrapの承認・finalizeまで完了し、**Select resident** でactiveなResidentを選びます。 |
| `memory_policy_not_service_current` | 有効な旧v2／v3 policyはv4への移行が必要です。[記憶とself-talk](configuration.md#記憶とself-talk)を参照してください。初期v1の通常対話には移行不要です。 |
| データがbusy／locked、`Stop dialogue first` | 対話を停止し、**Stopped** まで待ちます。別ターミナルのサーバーが同じ保存先を使っていれば、そちらも起動元で終了します。 |
| listen／bindでアドレス使用中 | 同じポートを使うプロセスがないか確認します。管理と対話の既定ポートは8788と8787です。 |
| shutdown失敗・`restart_required` | 新しい対話を重ねて起動せず、管理サーバーを終了してから起動し直します。 |
| filesystemがunsafe／unsupported | 対応するローカルファイルシステムと所有者・権限を確認します。ロックや安全確認を無効にして続行しないでください。 |

`memory_policy_not_service_current` 以外のreadinessエラーには別の原因があります。表示された理由を残し、**Diagnostics** など対応する操作の結果を確認してください。

## 「Generating response (attempt 1)」が出る

今回の応答を1回目として生成している通常の進捗表示です。送信回数ではありません。生成サービスへの再試行があるとattempt番号が増えます。

現行UIでは、その応答の正常完了時に表示が消えます。返信が表示された後も残る場合は古いサーバーが動いていないか確認してください。更新後は[サーバーの再起動](operations.md#停止と再起動)が必要です。返信自体が来ない場合は、生成サービス・接続・タイムアウトや後続のエラー表示を確認します。

## self-talkが始まらない／画面に出ない

self-talkは内部処理で、対話画面に出ないのが通常です。有効化は[設定手順](configuration.md#記憶とself-talk)に従います。

**Autonomy status** の抑止理由を確認してください。`disabled` は設定がOFF、`quiet_hours` は静穏時間、`minimum_interval` は待機時間、`no_trigger` は起点不足などを示します。v4適用後のユーザーメッセージ、選択中のResident、対話サーバーの稼働も必要です。`effective` だけでサーバーが動いているとは判断しないでください。

## Tailscaleから開けない／送信時に403になる

[設定手順](web-administration.md#access-dialogue-through-your-tailscale-network)にあるdialogue専用フラグで起動したか、Tailscale Serveが対話の8787へ転送しているか確認します。管理画面の8788はremote許可の対象外です。

対話を表示できても、送信時はOriginのホスト名とポートがHostに一致する必要があります。プロキシが外側のHostを維持しているか確認してください。`Forwarded` 系ヘッダーや任意CORS許可で代用する設定ではありません。

## Residentや設定が見つからない

管理画面の **Data directory** と、起動時の `--config` / `--data-dir` を確認します。Windowsでは設定はRoaming、データはLocalという別の場所が既定です。[保存先の一覧](configuration.md#設定ファイルとデータの保存先)を参照してください。

フォルダ削除やBootstrapのやり直しを先に行わず、元の保存先を開いているか確認します。復元する場合も、[バックアップの手順](operations.md#バックアップと復元)で新しい保存先へ復元します。

## 設定変更や画面更新が反映されない

ブラウザーの再読み込みだけでは、稼働中サーバーの実行ファイル・起動フラグ・環境変数は変わりません。[設定の反映表](configuration.md#設定の反映)に従って再起動する範囲を確認してください。
