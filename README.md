# まほろば (Mahoroba)

Residentとの対話や記憶をローカルに保存するアプリケーションです。このフォルダーは、ローカルビルドまたはDocker Desktopで利用するためのソース配布版です。

**応答生成には、別途Chat Completions互換の生成サービスが必要です。** モデルや生成サービス、APIキーは同梱していません。初回ビルドには依存パッケージやコンテナイメージを取得するネットワーク接続が必要です。

## はじめる

| 実行方法 | 必要なもの | 手順 |
| --- | --- | --- |
| Windowsでローカルビルド | Go 1.26.6 | [導入と最初の対話](docs/guide/getting-started.md) |
| Dockerで実行 | Docker Desktop（Linuxコンテナ） | [Dockerで使う](docs/guide/docker.md) |

どちらも、このREADMEと `go.mod` があるフォルダーで作業します。GitリポジトリやGitHubアカウントは不要です。

ローカル版は次のコマンドでビルドします。

```powershell
go build -trimpath -buildvcs=false -o mahoroba.exe ./cmd/mahoroba
if (-not (Test-Path .\config.toml)) {
    Copy-Item .\config.example.toml .\config.toml
}
```

`config.toml` の生成サービスURL・モデルを設定した後、次のコマンドで管理画面を起動します。APIキーが必要な場合は[導入ガイド](docs/guide/getting-started.md#2-設定を用意する)に従って環境変数へ設定してください。

```powershell
.\mahoroba.exe admin serve --config .\config.toml
```

Docker版は `.env.example` を `.env` にコピーし、生成サービスURL・モデル・必要なAPIキーを設定してから起動します。

```powershell
docker compose up -d --build
```

管理画面の既定URLは [http://127.0.0.1:8788/](http://127.0.0.1:8788/) です。**Bootstrap init → approve → finalize** でResidentを作り、**Start dialogue server → Open dialogue** から会話を始めます。詳しい設定は各ガイドを参照してください。

## 同梱内容

- `cmd/`・`internal/`：アプリのソース、画面資産、データベースmigration、通常テスト。
- `docs/guide/`：導入・日常操作・設定・トラブル解決のガイド。
- `Dockerfile`・`compose.yaml`・`docker/`：現在のアプリを起動するコンテナ構成。
- `testdata/docs-baseline/`：migrationテストが参照する固定資料。利用者向けガイドではありません。
- [配布内容の説明](DISTRIBUTION.md)と `DISTRIBUTION-MANIFEST.json`：同梱範囲、変更内容、各ファイルのSHA-256。

通常の利用は[ドキュメント入口](docs/README.md)から進めてください。[ソースの検証](docs/testing.md)も参照できます。

このフォルダーには、利用済みの設定・会話データ・実行ファイル・キャッシュを含めていません。使い始めると生成される `.env`、`config.toml`、データベース、バックアップ、ログを、そのまま第三者へ渡さないでください。

## ライセンスと著作者表示

Copyright (C) 2026 Mahoroba contributors

本プロジェクトのコード・文書・テスト・ビルド設定のうち、別のライセンス表示がある部分を除く著作権の対象となる部分は、**GNU Affero General Public License version 3 only（SPDX: `AGPL-3.0-only`）**で提供します。将来のバージョンを自動的に許諾する指定ではありません。[LICENSE](LICENSE) が原文、[COPYRIGHT](COPYRIGHT) が本プロジェクトへの適用通知です。無保証で提供します。

第三者の著作権・ライセンスはそれぞれの条件を維持します。[THIRD_PARTY_LICENSES.txt](internal/releaseasset/THIRD_PARTY_LICENSES.txt)、[Web UI notices](internal/httpui/THIRD_PARTY_NOTICES.md)、各ファイル内の表示を参照してください。

再配布時は適用される著作権表示・ライセンス・第三者表示を保持し、改変箇所と変更日を明示してください。実行ファイルやコンテナを配布する場合は、その版の対応ソースをAGPLv3に従って提供してください。改変版をネットワーク経由で利用させる場合は、その利用者が対応ソースを無償で取得できる導線も必要です（[LICENSE](LICENSE) 第5・6・13条）。

プロジェクトの由来と公開準備の履歴は[配布内容の説明](DISTRIBUTION.md#由来と公開履歴)に記載しています。
