# はじめる

このガイドは、手元のソースとローカル実行ファイルを使って開始する手順です。WindowsのPowerShellを例にします。Docker Desktopを使う場合は[Dockerで使う](docker.md)を参照してください。

## 1. 実行ファイルを用意する

Go 1.26.6を用意し、ソースのルート（`go.mod` のあるフォルダ）で実行します。初回ビルドには依存パッケージの取得が必要です。

```powershell
go build -trimpath -buildvcs=false -o mahoroba.exe ./cmd/mahoroba
```

既存サーバーがこの実行ファイルを使用している場合は、先に[停止手順](operations.md#停止と再起動)を確認してください。

## 2. 設定を用意する

初めて設定する場合は、[設定例](../../config.example.toml)をコピーします。既存の設定ファイルはそのまま使えます。

```powershell
if (-not (Test-Path .\config.toml)) {
    Copy-Item .\config.example.toml .\config.toml
}
```

`config.toml` の次の値を使用する生成サービスに合わせます。

| 項目 | 設定内容 |
| --- | --- |
| `timezone` | 例：`Asia/Tokyo`。静穏時間帯などにも使います。 |
| `generation.base_url` | Chat Completions互換APIのベースURL。`/chat/completions` はアプリが追加します。 |
| `generation.model` | サービスが受け付けるモデル名。 |
| `data_dir` | 空文字ならOS別の既定保存先。変更する場合は絶対パス。 |

APIキーが必要なサービスでは、起動するシェルの `MAHOROBA_PROVIDER_API_KEY` に設定します。TOMLには書きません。PowerShellでは、値を画面に表示せず入力できます。

```powershell
$providerKey = Read-Host "Provider API key" -AsSecureString
$env:MAHOROBA_PROVIDER_API_KEY = [System.Net.NetworkCredential]::new("", $providerKey).Password
Remove-Variable providerKey
```

設定の場所・優先順位・反映方法は[設定](configuration.md)を参照してください。

## 3. 管理画面を開く

```powershell
.\mahoroba.exe admin serve --config .\config.toml
```

[管理画面 http://127.0.0.1:8788/](http://127.0.0.1:8788/)を開きます。このコマンドを実行している間、管理サーバーが動きます。Residentやデータベースがまだなくても管理画面を起動できます。

## 4. 最初のResidentを作る

管理画面の **Resident** カテゴリで、順に実行します。

1. **Bootstrap init**：所有者名、Resident名、seed、Principlesを確認して実行し、結果の `resident_id` を控えます。
2. **Bootstrap approve**：そのResident IDを指定し、Principlesの承認を確認して実行します。
3. **Bootstrap finalize**：同じIDを指定し、Personaと初期Memory policyを確認して実行します。**Select this resident after activation** は既定でONです。

Principlesは作成時、Personaは有効化時に指定します。Personaは入力した文章が使われ、Principlesから自動生成されるわけではありません。初期Memory policyは既定のままで通常の対話を始められます。記憶のRecallやself-talkを使う場合は、後で[有効化手順](configuration.md#記憶とself-talk)を実施します。

## 5. 対話を始める

管理画面の **Start dialogue server** を実行し、**Running** になったら **Open dialogue** を開きます。既定の対話画面は [http://127.0.0.1:8787/](http://127.0.0.1:8787/) です。

**Shift+Enterで送信、Enterで改行**します。送信ボタンでも送れます。起動に失敗した場合は管理画面に理由が表示されます。[困ったとき](troubleshooting.md)を参照してください。

続きは[日常操作](operations.md)、全操作の詳細は[Web管理ガイド](web-administration.md)へ進んでください。
