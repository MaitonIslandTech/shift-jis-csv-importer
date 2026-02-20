# Shift JIS CSV Importer

Shift JIS形式のCSVファイルをSQL Serverに高速インポートするPowerShellスクリプト集です。

## 概要

このプロジェクトは、日本語を含むShift JIS形式のCSVファイルをSQL Serverに高速かつ効率的にインポートするためのPowerShellスクリプトを提供します。

## 主な機能

✅ **Shift JISエンコーディング対応** - 日本語を含むCSVファイルに対応  
✅ **高速処理** - SqlBulkCopyで数千行/秒を実現  
✅ **エラーハンドリング** - エラー行を自動的に記録して処理を継続  
✅ **詳細ログ出力** - 実行状況を日付付きログに記録  
✅ **エラー行の分離** - 問題のある行を別ファイルに出力  

## ファイル構成

- `Import-ShiftJisCSVToSQLServer-WithLogging.ps1` - メインスクリプト（ロギング・エラー処理機能付き）

## 必要な環境

- Windows PowerShell 5.0以上
- SQL Server
- .NET Framework 4.6以上

## インストール

リポジトリをクローンします：

```bash
git clone https://github.com/MaitonIslandTech/shift-jis-csv-importer.git
cd shift-jis-csv-importer
```

## 使用方法

### 基本的な実行

```powershell
.\