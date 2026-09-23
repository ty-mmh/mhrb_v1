# ソースの検証

この配布版には、通常アプリの単体・統合テストを含めています。過去のCIワークフローやリリース証明ツールは含みません。

Go 1.26.6を使用し、ソースのルートで実行します。テスト用データは実際の利用者データとは別に作成されます。Goの依存取得にはネットワーク接続が必要な場合があります。

## Windows

Windowsでは、ファイル権限の境界を検証するため、専用の保護された一時領域を使います。Goは標準の `Program Files\Go` にインストールしてください。新しいPowerShellを開き、次を実行します。

```powershell
$testTemp = ./scripts/prepare_m7_windows_local_test_temp.ps1
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($testTemp)) {
    throw "Test temporary directory preparation failed."
}
$env:TMP = $testTemp
$env:TEMP = $testTemp
$env:TMPDIR = $testTemp
$env:GOTMPDIR = $testTemp
$env:MAHOROBA_CI_WINDOWS_TEST_TEMP = $testTemp
go test ./...
```

終了後はこのPowerShellを閉じます。環境変数の指定はこのシェルと、そこから起動したプロセスにだけ適用されます。

## Linux

```sh
go test ./...
```

ローカル実行ファイルを作るときは、ビルドした端末のパスが埋め込まれないよう、[導入ガイド](guide/getting-started.md)の `-trimpath -buildvcs=false` を使います。

migrationの固定資料は [testdata/docs-baseline](../testdata/docs-baseline/README.md) に分離しています。実際に組み込むSQLとの内容・ハッシュ比較を継続して行います。
