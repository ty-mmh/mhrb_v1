# まほろば 利用ガイド

この入口では、現在のローカル版まほろばを使うための手順を案内します。実行ファイルを使う場合は[導入と最初の対話](guide/getting-started.md)、Docker Desktopを使う場合は[Dockerで使う](guide/docker.md)から進めてください。

## 目的から探す

| 目的 | 読む資料 |
| --- | --- |
| 設定を用意し、residentを作って会話する | [導入と最初の対話](guide/getting-started.md) |
| Docker Desktopで管理・対話画面を起動する | [Dockerで使う](guide/docker.md) |
| サーバーを起動・停止する、residentを管理する | [日常操作](guide/operations.md) |
| プロバイダーや保存先を設定する、self-talkやTailscaleを使う | [設定](guide/configuration.md) |
| Bootstrap、起動、接続で困ったとき | [トラブル解決](guide/troubleshooting.md) |
| 管理画面の操作一覧や細かい挙動を調べる | [Web administration](guide/web-administration.md) |

設定項目の全体は[設定ファイルの例](../config.example.toml)を参照してください。

## 保存している資料

過去の開発記録、内部設計、監査資料、今後の開発案は[アーカイブ](archive/README.md)から参照できます。[旧パスと現在の保存先](archive/paths.md)では、整理前の資料をファイル単位で探せます。

`database/sqlite/v0.1.3/`、`assurance/`、`design/`に残している40ファイルは、現行テストが参照する検証資料です。固定fixture1件はリポジトリルートの `internal/assurance/testdata/`、過去の検証補助ツール2件は `tools/historical/` に分けています。[検証用資料の一覧](archive/paths.md#retained-validation)に用途を記載しています。通常の導入や操作で読む必要はありません。

アーカイブへの移動は資料の削除や計画の廃止を意味しません。資料中の版、commit、検証結果は作成時点の記録として読み、現在の動作は利用ガイドと実装で確認してください。
