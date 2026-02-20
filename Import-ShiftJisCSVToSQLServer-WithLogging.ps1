# Shift JIS形式のCSVを高速インポート
# エラー行をエラーファイルに、実行状況をログに記録

param(
    [string]$csvFilePath = "C:\data\input.csv",
    [string]$serverName = "localhost",
    [string]$databaseName = "YourDatabase",
    [string]$tableName = "YourTable",
    [string]$sqlLogin = "sa",
    [string]$sqlPassword = "YourPassword",
    [int]$batchSize = 10000,
    [int]$timeoutSeconds = 300,
    [string]$logDirectory = "C:\logs",
    [string]$errorDirectory = "C:\logs\errors"
)

# ============================================================================
# ロギングヘルパー関数
# ============================================================================

function Initialize-Logging {
    param(
        [string]$LogDir,
        [string]$ErrorDir
    )

    # ディレクトリ作成
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    if (-not (Test-Path $ErrorDir)) {
        New-Item -ItemType Directory -Path $ErrorDir -Force | Out-Null
    }

    # ログファイルパスを日付付きで作成
    $timestamp = Get-Date -Format "yyyyMMdd"
    $timeHMS = Get-Date -Format "HHmmss"
    
    $logFile = Join-Path $LogDir "import_${timestamp}_${timeHMS}.log"
    $errorFile = Join-Path $ErrorDir "error_rows_${timestamp}_${timeHMS}.csv"

    return @{
        LogFile = $logFile
        ErrorFile = $errorFile
        LogDir = $LogDir
        ErrorDir = $ErrorDir
    }
}

function Write-Log {
    param(
        [string]$Message,
        [string]$LogFile,
        [ValidateSet("INFO", "WARN", "ERROR", "SUCCESS")]
        [string]$Level = "INFO"
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $logMessage = "[$timestamp] [$Level] $Message"

    # コンソールに出力
    switch ($Level) {
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "WARN" { Write-Host $logMessage -ForegroundColor Yellow }
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        default { Write-Host $logMessage -ForegroundColor White }
    }

    # ログファイルに書き込み
    Add-Content -Path $LogFile -Value $logMessage -Encoding UTF8
}

function Write-ErrorRow {
    param(
        [string]$ErrorFile,
        [string]$LineNumber,
        [string]$RawLine,
        [string]$ErrorMessage,
        [string[]]$Headers,
        [bool]$IsHeaderRow = $false
    )

    if ($IsHeaderRow) {
        # ヘッダー行をエラーファイルに書き込み（1回だけ）
        $headerLine = ($Headers -join ',')
        Add-Content -Path $ErrorFile -Value $headerLine -Encoding UTF8
        return
    }

    # エラー行をエラーファイルに記録
    $errorEntry = "$LineNumber,`"$([datetime]::Now.ToString('yyyy-MM-dd HH:mm:ss'))`",`"$ErrorMessage`",`"$RawLine`""
    Add-Content -Path $ErrorFile -Value $errorEntry -Encoding UTF8
}

# ============================================================================
# メイン処理
# ============================================================================

function Import-ShiftJisCSVFastWithLogging {
    param(
        [string]$FilePath,
        [string]$ServerName,
        [string]$DatabaseName,
        [string]$TableName,
        [string]$SqlLogin,
        [string]$SqlPassword,
        [int]$BatchSize,
        [int]$TimeoutSeconds,
        [string]$LogDirectory,
        [string]$ErrorDirectory
    )

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    # ロギング初期化
    $logPaths = Initialize-Logging -LogDir $LogDirectory -ErrorDir $ErrorDirectory
    $logFile = $logPaths.LogFile
    $errorFile = $logPaths.ErrorFile

    Write-Log "========================================" -LogFile $logFile -Level "INFO"
    Write-Log "Shift JIS CSV 高速インポート処理開始" -LogFile $logFile -Level "INFO"
    Write-Log "========================================" -LogFile $logFile -Level "INFO"
    Write-Log "ファイル: $FilePath" -LogFile $logFile
    Write-Log "サーバー: $ServerName" -LogFile $logFile
    Write-Log "データベース: $DatabaseName" -LogFile $logFile
    Write-Log "テーブル: $TableName" -LogFile $logFile
    Write-Log "バッチサイズ: $BatchSize" -LogFile $logFile
    Write-Log "ログファイル: $logFile" -LogFile $logFile
    Write-Log "エラーファイル: $errorFile" -LogFile $logFile
    Write-Log "" -LogFile $logFile

    try {
        # ファイルの存在確認
        if (-not (Test-Path $FilePath)) {
            Write-Log "ファイルが見つかりません: $FilePath" -LogFile $logFile -Level "ERROR"
            return $false
        }

        # ファイル情報取得
        $fileInfo = Get-Item $FilePath
        Write-Log "ファイルサイズ: $([math]::Round($fileInfo.Length / 1MB, 2)) MB" -LogFile $logFile

        # SQL Server接続情報
        $connectionString = "Server=$ServerName;Database=$DatabaseName;User Id=$SqlLogin;Password=$SqlPassword;"

        Write-Log "SQL Serverに接続中..." -LogFile $logFile

        $sqlConnection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        
        try {
            $sqlConnection.Open()
            Write-Log "SQL Serverに接続しました" -LogFile $logFile -Level "SUCCESS"
        }
        catch {
            Write-Log "SQL Server接続エラー: $_" -LogFile $logFile -Level "ERROR"
            return $false
        }

        # SqlBulkCopyの初期化
        $sqlBulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($sqlConnection)
        $sqlBulkCopy.DestinationTableName = $TableName
        $sqlBulkCopy.BatchSize = $BatchSize
        $sqlBulkCopy.BulkCopyTimeout = $TimeoutSeconds
        $sqlBulkCopy.EnableStreaming = $true

        # データテーブルの作成
        $dataTable = New-Object System.Data.DataTable

        Write-Log "CSVファイルを読み込み中..." -LogFile $logFile

        # Shift JISでファイルを読み込み
        $streamReader = New-Object System.IO.StreamReader($FilePath, [System.Text.Encoding]::GetEncoding('Shift_JIS'))
        
        # ヘッダー行を読み込み
        $headerLine = $streamReader.ReadLine()
        if ([string]::IsNullOrEmpty($headerLine)) {
            Write-Log "CSVファイルが空です" -LogFile $logFile -Level "ERROR"
            return $false
        }

        # ヘッダーをパース
        $headers = @()
        try {
            $headers = $headerLine -split ',' | ForEach-Object { $_.Trim('"').Trim() }
            Write-Log "ヘッダー行解析完了: $($headers.Count) 列" -LogFile $logFile
        }
        catch {
            Write-Log "ヘッダー行解析エラー: $_" -LogFile $logFile -Level "ERROR"
            return $false
        }

        # DataTableにカラムを追加
        foreach ($header in $headers) {
            $dataTable.Columns.Add($header, [string]) | Out-Null
        }

        # 列マッピングを設定
        for ($i = 0; $i -lt $headers.Count; $i++) {
            $mapping = New-Object System.Data.SqlClient.SqlBulkCopyColumnMapping($i, $headers[$i])
            $sqlBulkCopy.ColumnMappings.Add($mapping) | Out-Null
        }

        # エラーファイルにヘッダーを書き込み
        Write-ErrorRow -ErrorFile $errorFile -Headers @("LineNumber", "Timestamp", "ErrorMessage", "RawData") -IsHeaderRow $true

        Write-Log "データ読み込み・投入中..." -LogFile $logFile
        Write-Log "" -LogFile $logFile

        $rowCount = 0
        $errorCount = 0
        $lineNumber = 1  # ヘッダーは1行目
        $lastProgressTime = [System.DateTime]::Now

        # データ行を処理
        while ($null -ne ($line = $streamReader.ReadLine())) {
            $lineNumber++

            if ([string]::IsNullOrEmpty($line)) { continue }

            try {
                # CSV行をパース
                $values = @()
                $inQuotes = $false
                $currentValue = ""
                
                for ($i = 0; $i -lt $line.Length; $i++) {
                    $char = $line[$i]
                    
                    if ($char -eq '"') {
                        if ($inQuotes -and $i + 1 -lt $line.Length -and $line[$i + 1] -eq '"') {
                            $currentValue += '"'
                            $i++
                        }
                        else {
                            $inQuotes = -not $inQuotes
                        }
                    }
                    elseif ($char -eq ',' -and -not $inQuotes) {
                        $values += $currentValue.Trim()
                        $currentValue = ""
                    }
                    else {
                        $currentValue += $char
                    }
                }
                $values += $currentValue.Trim()

                # 列数チェック
                if ($values.Count -ne $headers.Count) {
                    $errorMsg = "列数エラー: 期待値=$($headers.Count), 実際=$($values.Count)"
                    Write-ErrorRow -ErrorFile $errorFile -LineNumber $lineNumber -RawLine $line -ErrorMessage $errorMsg
                    $errorCount++
                    continue
                }

                # 行をDataTableに追加
                $row = $dataTable.NewRow()
                for ($i = 0; $i -lt $headers.Count; $i++) {
                    $row[$i] = if ([string]::IsNullOrEmpty($values[$i])) { [DBNull]::Value } else { $values[$i] }
                }
                $dataTable.Rows.Add($row)
                $rowCount++

                # バッチサイズに達したら投入
                if ($rowCount % $BatchSize -eq 0) {
                    try {
                        $sqlBulkCopy.WriteToServer($dataTable)
                        $rowsPerSec = [math]::Round($rowCount / $stopwatch.Elapsed.TotalSeconds, 0)
                        Write-Log "投入済み: $rowCount 行 | エラー: $errorCount 行 | 速度: $rowsPerSec 行/秒" -LogFile $logFile
                        $dataTable.Clear()
                    }
                    catch {
                        Write-Log "バッチ投入エラー (行$rowCount): $_" -LogFile $logFile -Level "ERROR"
                        return $false
                    }
                }
            }
            catch {
                $errorMsg = $_.Exception.Message
                Write-ErrorRow -ErrorFile $errorFile -LineNumber $lineNumber -RawLine $line -ErrorMessage $errorMsg
                $errorCount++
            }
        }

        # 残りのデータを投入
        if ($dataTable.Rows.Count -gt 0) {
            try {
                $sqlBulkCopy.WriteToServer($dataTable)
                Write-Log "最終バッチ投入: $($dataTable.Rows.Count) 行" -LogFile $logFile
            }
            catch {
                Write-Log "最終バッチ投入エラー: $_" -LogFile $logFile -Level "ERROR"
                return $false
            }
        }

        $streamReader.Close()
        $stopwatch.Stop()

        Write-Log "" -LogFile $logFile
        Write-Log "========================================" -LogFile $logFile -Level "SUCCESS"
        Write-Log "インポート完了" -LogFile $logFile -Level "SUCCESS"
        Write-Log "========================================" -LogFile $logFile -Level "SUCCESS"
        Write-Log "処理対象行数: $lineNumber (ヘッダー含む)" -LogFile $logFile
        Write-Log "正常投入行数: $rowCount" -LogFile $logFile -Level "SUCCESS"
        Write-Log "エラー行数: $errorCount" -LogFile $logFile $(if ($errorCount -gt 0) { "WARN" } else { "SUCCESS" })
        Write-Log "実行時間: $($stopwatch.Elapsed.TotalSeconds) 秒" -LogFile $logFile
        Write-Log "平均速度: $([math]::Round($rowCount / $stopwatch.Elapsed.TotalSeconds, 0)) 行/秒" -LogFile $logFile
        Write-Log "========================================" -LogFile $logFile

        if ($errorCount -gt 0) {
            Write-Log "エラー詳細は以下のファイルを確認してください: $errorFile" -LogFile $logFile -Level "WARN"
        }

        return $true

    }
    catch {
        Write-Log "予期しないエラーが発生しました: $_" -LogFile $logFile -Level "ERROR"
        Write-Log "スタックトレース: $($_.ScriptStackTrace)" -LogFile $logFile -Level "ERROR"
        return $false

    }
    finally {
        if ($null -ne $sqlBulkCopy) {
            $sqlBulkCopy.Close()
        }
        if ($null -ne $sqlConnection -and $sqlConnection.State -eq "Open") {
            $sqlConnection.Close()
        }
        if ($null -ne $streamReader) {
            $streamReader.Close()
        }
    }
}

# ============================================================================
# スクリプト実行
# ============================================================================

Write-Host ""
Write-Host "Shift JIS CSV インポート処理を開始します..." -ForegroundColor Cyan
Write-Host ""

$result = Import-ShiftJisCSVFastWithLogging -FilePath $csvFilePath `
                                             -ServerName $serverName `
                                             -DatabaseName $databaseName `
                                             -TableName $tableName `
                                             -SqlLogin $sqlLogin `
                                             -SqlPassword $sqlPassword `
                                             -BatchSize $batchSize `
                                             -TimeoutSeconds $timeoutSeconds `
                                             -LogDirectory $logDirectory `
                                             -ErrorDirectory $errorDirectory

Write-Host ""
if ($result) {
    Write-Host "処理が正常に完了しました" -ForegroundColor Green
    exit 0
}
else {
    Write-Host "処理に失敗しました" -ForegroundColor Red
    exit 1
}