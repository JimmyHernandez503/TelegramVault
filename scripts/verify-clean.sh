#!/bin/bash
# Script to verify the project is clean and ready for GitHub

set -e

echo "🔍 Verifying TelegramVault is GitHub-ready..."
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

# Check for sensitive files
echo "📁 Checking for sensitive files..."
SENSITIVE_FILES=(
    ".env"
    "*.session"
    "*.log"
    "*.db"
    "*.sqlite"
)

for pattern in "${SENSITIVE_FILES[@]}"; do
    if find . -name "$pattern" -not -path "./node_modules/*" -not -path "./.venv/*" | grep -q .; then
        echo -e "${RED}✗ Found sensitive files matching: $pattern${NC}"
        find . -name "$pattern" -not -path "./node_modules/*" -not -path "./.venv/*"
        ERRORS=$((ERRORS + 1))
    fi
done

# Check for hardcoded secrets
echo ""
echo "🔐 Checking for hardcoded secrets..."

# Check for actual API IDs (not examples)
if grep -r "TELEGRAM_API_ID=[0-9]\{8,\}" --exclude-dir=node_modules --exclude-dir=.venv --exclude="*.md" --exclude=".env.example" . 2>/dev/null; then
    echo -e "${RED}✗ Found hardcoded Telegram API ID${NC}"
    ERRORS=$((ERRORS + 1))
fi

# Check for actual API hashes (not placeholders)
if grep -r "TELEGRAM_API_HASH=[a-f0-9]\{32\}" --exclude-dir=node_modules --exclude-dir=.venv --exclude="*.md" --exclude=".env.example" . 2>/dev/null; then
    echo -e "${RED}✗ Found hardcoded Telegram API Hash${NC}"
    ERRORS=$((ERRORS + 1))
fi

# Check for actual database URLs with passwords
if grep -r "postgresql://.*:.*@" --exclude-dir=node_modules --exclude-dir=.venv --exclude="*.md" --exclude=".env.example" --exclude="docker-compose.yml" . 2>/dev/null; then
    echo -e "${YELLOW}⚠ Found database URL with credentials (verify it's an example)${NC}"
    WARNINGS=$((WARNINGS + 1))
fi

# Check for required files
echo ""
echo "📄 Checking for required files..."
REQUIRED_FILES=(
    "README.md"
    "LICENSE"
    ".gitignore"
    ".env.example"
    "docker-compose.yml"
    "Dockerfile"
    "pyproject.toml"
    "CONTRIBUTING.md"
    "CHANGELOG.md"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ Missing required file: $file${NC}"
        ERRORS=$((ERRORS + 1))
    else
        echo -e "${GREEN}✓ Found: $file${NC}"
    fi
done

# Check for empty directories with .gitkeep
echo ""
echo "📂 Checking empty directories..."
EMPTY_DIRS=(
    "media"
    "sessions"
    "logs"
)

for dir in "${EMPTY_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        if [ ! -f "$dir/.gitkeep" ]; then
            echo -e "${YELLOW}⚠ Directory $dir exists but missing .gitkeep${NC}"
            WARNINGS=$((WARNINGS + 1))
        else
            echo -e "${GREEN}✓ $dir has .gitkeep${NC}"
        fi
    fi
done

# Check for __pycache__ directories
echo ""
echo "🧹 Checking for Python cache..."
if find . -type d -name "__pycache__" | grep -q .; then
    echo -e "${YELLOW}⚠ Found __pycache__ directories${NC}"
    find . -type d -name "__pycache__"
    WARNINGS=$((WARNINGS + 1))
fi

# Check for node_modules
echo ""
echo "📦 Checking for node_modules..."
if [ -d "client/node_modules" ]; then
    echo -e "${YELLOW}⚠ Found client/node_modules (should be in .gitignore)${NC}"
    WARNINGS=$((WARNINGS + 1))
fi

# Check .gitignore
echo ""
echo "🚫 Verifying .gitignore..."
GITIGNORE_PATTERNS=(
    "*.log"
    ".env"
    "node_modules/"
    "__pycache__/"
    "media/"
    "sessions/"
)

for pattern in "${GITIGNORE_PATTERNS[@]}"; do
    if ! grep -q "$pattern" .gitignore; then
        echo -e "${RED}✗ .gitignore missing pattern: $pattern${NC}"
        ERRORS=$((ERRORS + 1))
    fi
done

# Summary
echo ""
echo "================================"
if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✅ All checks passed! Project is GitHub-ready.${NC}"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠️  $WARNINGS warning(s) found. Review before publishing.${NC}"
    exit 0
else
    echo -e "${RED}❌ $ERRORS error(s) and $WARNINGS warning(s) found.${NC}"
    echo -e "${RED}Please fix errors before publishing to GitHub.${NC}"
    exit 1
fi
