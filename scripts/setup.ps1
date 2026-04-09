# setup.ps1 - Basic environment setup script

Write-Host "Hello, World!"
Write-Host "Setting up your environment..."

# Check Python is available
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "Python not found. Please install Python before continuing."
    exit 1
}

Write-Host "Python found: $(python --version)"
Write-Host "Setup complete."
