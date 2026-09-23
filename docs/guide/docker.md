# Dockerで使う

WindowsのDocker Desktopで、管理画面と対話画面を動かす手順です。同梱構成はLinux AMD64コンテナ固定で、ARM64向けの構成ではありません。ソースのルート（`compose.yaml` のあるフォルダ）でPowerShellから実行してください。ホストへのGoのインストールやGit情報は不要です。

この構成はDocker専用のデータ領域を使います。ローカル実行版のResidentや設定は自動では取り込みません。初回ビルドには、ベースイメージとGo依存パッケージを取得するためのネットワーク接続が必要です。

## 1. 生成サービスとポートを設定する

Docker Desktopを起動し、設定例をコピーします。既存の `.env` がある場合は内容を確認して使います。

```powershell
if (-not (Test-Path .\.env)) {
    Copy-Item .\.env.example .\.env
}
```

`.env` を編集します。

| 項目 | 設定内容 |
| --- | --- |
| `MAHOROBA_PROVIDER_BASE_URL` | Chat Completions互換APIのベースURL。末尾の `/chat/completions` はアプリが追加します。 |
| `MAHOROBA_PROVIDER_MODEL` | 使用するサービスのモデル名。例の仮の値を置き換えます。 |
| `MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP` | Docker版では `true`。ホスト上の生成サービスへの限定したHTTP接続を許可します。HTTPSサービスを使う場合は `false` にできます。 |
| `MAHOROBA_PROVIDER_API_KEY` | 認証が必要な場合だけ設定します。不要なら行自体を省略します。空文字は指定しません。 |
| `MAHOROBA_ADMIN_PORT` | 管理画面のポート。既定は `8788`。 |
| `MAHOROBA_DIALOGUE_PORT` | 対話画面のポート。既定は `8787`。 |
| `MAHOROBA_DIALOGUE_ALLOW_REMOTE` | 通常は既定の `false`。Tailscaleから対話する場合だけ、後述の手順で `true` にします。 |

APIキーを設定した `.env` は共有しないでください。このファイルはイメージにコピーされず、コンテナ作成時に必要な環境変数として渡されます。起動シェルに同名の環境変数がある場合は、そちらが `.env` より優先されます。

ホストのWindows上で生成サービスを動かしている場合は、たとえば `http://host.docker.internal:8080/v1` のように指定します。実際のサービスが受け付けるパスに合わせてください。コンテナ内の `127.0.0.1` はコンテナ自身を指します。Docker Desktopからホストへの接続には `host.docker.internal` を使います。[Dockerの接続案内](https://docs.docker.com/desktop/features/networking/networking-how-tos/)

HTTPで接続する場合は、`MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP=true` と、正確なホスト名 `host.docker.internal`、明示した有効なポート番号が必要です。追加の許可対象はこの生成サービス接続だけで、別のホスト名・ホスト名の別名・TTSには適用されません。この接続ではHTTPプロキシとリダイレクトを使いません。通常のローカル実行版ではこの許可は既定でOFFです。画面の待受を許可する `--container-listen` とは別の設定です。

ローカル実行版が既に `8788` / `8787` を使っている場合は、そちらを停止するか、Docker側を次のような空きポートに変更します。管理と対話には異なるポートを指定してください。

```dotenv
MAHOROBA_ADMIN_PORT=18788
MAHOROBA_DIALOGUE_PORT=18787
```

ポートを変えた場合は、以下のブラウザーURLもその番号に読み替えます。ホスト側の公開ポート、コンテナ側の待受、管理画面の対話リンクは一緒に変更されます。

## 2. 管理画面を起動する

```powershell
docker compose up -d --build
docker compose ps
```

[管理画面 http://127.0.0.1:8788/](http://127.0.0.1:8788/)を開きます。コンテナの `healthy` は管理サーバーが応答するという意味です。Resident作成前や対話サーバーが **Stopped** のときも正常です。

管理画面・対話画面は、既定ではホストの `127.0.0.1` にだけ公開します。コンテナ内で待ち受けるための `--container-listen` は、外部ホスト名からのアクセスを許可するフラグではありません。Host・Originの検査は引き続き有効です。

## 3. Residentを作り、対話する

管理画面の **Resident** で、次の順に実行します。

1. **Bootstrap init**：所有者名、Resident名、seed、Principlesを指定し、結果の `resident_id` を控えます。
2. **Bootstrap approve**：そのIDを指定し、Principlesを承認します。
3. **Bootstrap finalize**：同じIDのPersonaと初期Memory policyを確認して実行します。**Select this resident after activation** をONにします。
4. **Start dialogue server** → **Running** → **Open dialogue** の順に進みます。

既定の対話画面は [http://127.0.0.1:8787/](http://127.0.0.1:8787/) です。**Shift+Enterで送信、Enterで改行**します。

既にDocker側にResidentを作成済みなら、Bootstrapのやり直しは不要です。コンテナ起動後に管理画面から対話をStartします。Residentの切り替えや記憶・self-talkの条件は[日常操作](operations.md)と[設定](configuration.md#記憶とself-talk)を参照してください。

## 停止・再開・更新

対話だけを止める場合は管理画面の **Stop dialogue server** を使います。管理画面も止める場合は、次を実行します。

```powershell
docker compose stop
```

再開は次のコマンドです。その後、管理画面で **Start dialogue server** を実行します。

```powershell
docker compose up -d
```

| 変更内容 | 反映方法 |
| --- | --- |
| `.env` のプロバイダー・APIキー・ポートなど | `docker compose up -d --force-recreate` を実行し、管理画面から対話をStart。 |
| `docker/config.toml` のtimezone・self-talk・TTSなど | `docker compose up -d --build` を実行し、管理画面から対話をStart。 |
| アプリのソースコード | `docker compose up -d --build` を実行し、対話をStartしてブラウザーを再読み込み。 |

詳細設定は [docker/config.toml](../../docker/config.toml) に、[設定例](../../config.example.toml)から必要な項目を追加します。この設定ファイルはビルド時にイメージへコピーされるため、ホスト側の編集後に対話をStop / Startするだけでは反映されません。APIキーはこのTOMLへ書きません。

`docker compose restart` では、変更した環境変数やCompose設定は反映されません。コンテナを再作成しても、マウントしたnamed volumeのデータは保持されます。[Dockerの再起動仕様](https://docs.docker.com/reference/cli/docker/compose/restart/)、[再作成時の動作](https://docs.docker.com/reference/cli/docker/compose/up/)

## データとバックアップ

| 保存対象 | コンテナ内の場所 | 保存方法 |
| --- | --- | --- |
| Resident・履歴・記憶・blob | `/var/lib/mahoroba/data` | 親の `/var/lib/mahoroba` にComposeの `data` named volumeをマウント |
| バックアップ・export・消去計画など | `/var/lib/mahoroba-files` | Composeの `files` named volume |
| 詳細設定 | `/etc/mahoroba/config.toml` | ソースの `docker/config.toml` をイメージへコピー |

`data` volumeはデータ本体と、その親に作られるアプリのロックファイルを保持します。管理画面の **Data directory** には `/var/lib/mahoroba/data` と表示されます。

`docker compose stop` や通常の `docker compose down` ではnamed volumeは残ります。**`docker compose down -v` はデータと出力ファイルのvolumeも削除します。** データ保持が目的の停止では使いません。

Composeのプロジェクト名が変わると、別のvolumeを使います。作業フォルダの名前や `COMPOSE_PROJECT_NAME` を変更した後にResidentが見つからない場合は、元のプロジェクト・volumeを確認してください。

管理画面からバックアップを作る場合は、対話を停止し、**Backup and export → Create backup** の **Output absolute path** に未使用のパスを指定します。例：`/var/lib/mahoroba-files/backup-YYYYMMDD`。続けて **Verify backup** で同じパスを検証します。

ホストへ取り出す場合は、管理コンテナが存在する状態で、ソースルートから次を実行します。コピー先はまだ存在しない名前を選んでください。

```powershell
docker compose cp mahoroba:/var/lib/mahoroba-files/backup-YYYYMMDD ./backup-YYYYMMDD
```

UIで指定するパスはコンテナ内のLinuxパスです。Windowsのドライブパスは指定できません。**Download result** は操作結果テキストの保存で、バックアップ本体の取得ではありません。復元は新しい保存先へ行う操作です。詳細は[バックアップと復元](operations.md#バックアップと復元)を参照してください。

## Tailscaleから対話する場合

Tailscaleを使う場合だけ、`.env` の次の値を変更します。この変数はComposeが既存の `--dialogue-allow-remote` 引数に変換します。管理画面のHost・Origin制限や、ホストの `127.0.0.1` に限定したポート公開は変わりません。

```dotenv
MAHOROBA_DIALOGUE_ALLOW_REMOTE=true
```

`docker compose up -d --force-recreate` を実行し、ローカルの管理画面から対話をStartします。その後、Tailscaleに接続した**ホスト側**で対話ポートへ転送します。ポートを変更している場合は、`8787` を `MAHOROBA_DIALOGUE_PORT` の値に合わせてください。

```powershell
tailscale serve --bg --https=443 --set-path=/ --yes http://127.0.0.1:8787
tailscale serve status
```

このコマンドはHTTPSのルート `/` の転送先を設定・更新します。既に同じ対話ポートへ転送済みなら変更は不要です。表示されたtailnetのHTTPS URLから対話画面を開きます。**管理ポートは転送しません。** 自分のtailnet内で使うTailscale Serveを使用し、Funnelは使いません。[Tailscale Serveの仕様](https://tailscale.com/docs/reference/tailscale-cli/serve)

利用をやめる場合は `.env` を `false` に戻してコンテナを再作成します。転送も削除する場合はホストで `tailscale serve --bg --https=443 --set-path=/ --yes off` を実行します。アクセス許可はtailnet側で管理され、アプリにログイン機能が追加されるわけではありません。プロキシのHost・Origin条件は[Web管理ガイド](web-administration.md#access-dialogue-through-your-tailscale-network)を参照してください。

## 困ったとき

状態とログを確認します。

```powershell
docker compose ps
docker compose logs --tail 100 mahoroba
```

| 症状 | 確認すること |
| --- | --- |
| Dockerエンジンに接続できない | Docker Desktopが起動済みで、Linuxコンテナを使用しているか確認します。 |
| ポートが使用中 | ローカル版や別コンテナの使用状況を確認し、`.env` の2つのポートを空き番号へ変更して再作成します。 |
| 管理はhealthyだが対話を開けない | 管理画面でResidentの選択と **Start dialogue server** を確認します。healthyだけでは対話は開始されません。 |
| 生成サービスに接続できない | `.env` のURL、モデル、APIキーを確認します。ホスト上のHTTPサービスには `http://host.docker.internal:ポート番号` と追加のHTTP許可が必要です。サービス側の待受やファイアウォールも確認します。 |
| `.env` を変えたが反映されない | 同名のシェル環境変数による上書きを確認し、`up -d --force-recreate` で再作成します。 |
| `docker/config.toml` を変えたが反映されない | `up -d --build` でイメージを作り直します。 |
| Residentが見つからない | ローカル版とDocker版の保存先は別です。同じComposeプロジェクトの `data` volumeを使用しているか確認します。 |
| Tailscale経由で421／403になる | `.env` のremote許可を `true` にして再作成したか、ホスト側のServeが対話ポートへ転送しているか確認します。管理ポートはremote許可の対象外です。 |
| 出力先で権限エラー | UIでは `/var/lib/mahoroba-files/` 配下を指定します。実行ユーザーは非rootで、イメージ本体は読み取り専用です。 |

Bootstrapやmemory policyのエラーは[トラブル解決](troubleshooting.md)を参照してください。

この配布フォルダーの起動用資産は `Dockerfile` と `compose.yaml` です。過去の開発・CI・リリース検証用構成は含みません。
