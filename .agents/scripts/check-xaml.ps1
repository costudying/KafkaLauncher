# PostToolUse hook: validates edited XAML files for XML well-formedness
# Reads Antigravity hook JSON payload from stdin and prints "{}" to stdout.
$ErrorActionPreference = "SilentlyContinue"
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
$raw = [Console]::In.ReadToEnd()

try {
    if (-not [string]::IsNullOrWhiteSpace($raw)) {
        $payload = $raw | ConvertFrom-Json
        $targetPath = $payload.toolCall.args.Path
        if ($targetPath -and $targetPath.EndsWith(".xaml", [System.StringComparison]::OrdinalIgnoreCase) -and (Test-Path $targetPath)) {
            [xml]$doc = Get-Content -Raw -Encoding UTF8 $targetPath
        }
    }
}
catch {
    [Console]::Error.WriteLine("[xaml-syntax-guard] XAML XML validation failed: $($_.Exception.Message)")
}

Write-Output "{}"
