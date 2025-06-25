#!/bin/bash
# Development environment setup script

echo "Setting up France Data Collector development environment..."

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
required_version="3.11"

if [[ $(echo "$python_version >= $required_version" | bc) -eq 0 ]]; then
    echo "Error: Python $required_version or higher is required. Found: $python_version"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create necessary directories
echo "Creating directory structure..."
mkdir -p collectors/dvf collectors/sirene collectors/insee_contours collectors/plu
mkdir -p config tests utils scripts credentials logs

# Create .env file from example if it doesn't exist
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "Creating .env file from example..."
        cp .env.example .env
        echo "Please edit .env file with your actual configuration values"
    fi
fi

# Run validation
echo ""
echo "Running validation..."
python scripts/validate_setup.py

echo ""
echo "Setup complete! Next steps:"
echo "1. Edit .env file with your Google Cloud configuration"
echo "2. Place your service account key in credentials/service-account-key.json"
echo "3. Run 'source venv/bin/activate' to activate the virtual environment"
echo "4. Run 'python scripts/validate_setup.py' to verify your setup"