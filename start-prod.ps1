Set-Location $PSScriptRoot

$python = "..\.venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
  $python = "python"
}

& $python -m pip install -r requirements.txt
& $python -m waitress --host=0.0.0.0 --port=5050 app:app
