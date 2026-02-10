#!/bin/bash
# Script to prepare a clean release of TelegramVault

set -e

echo "🚀 Preparing TelegramVault for release..."
echo ""

# Remove development files
echo "🧹 Cleaning development files..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "node_modules" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true
find . -type f -name "*.log" -delete 2>/dev/null || true
find . -type f -name ".DS_Store" -delete 2>/dev/null || true

# Remove sensitive files
echo "🔒 Removing sensitive files..."
rm -f .env 2>/dev/null || true
rm -f *.session 2>/dev/null || true
rm -f *.db 2>/dev/null || true
rm -f *.sqlite 2>/dev/null || true

# Clean media directory but keep structure
echo "📁 Cleaning media directory..."
if [ -d "media" ]; then
    find media -type f -delete 2>/dev/null || true
    find media -type d -empty -exec touch {}/.gitkeep \; 2>/dev/null || true
fi

# Clean sessions directory
echo "🔑 Cleaning sessions directory..."
if [ -d "sessions" ]; then
    rm -rf sessions/* 2>/dev/null || true
    touch sessions/.gitkeep
fi

# Clean logs directory
echo "📝 Cleaning logs directory..."
if [ -d "logs" ]; then
    rm -rf logs/* 2>/dev/null || true
    touch logs/.gitkeep
fi

# Verify .env.example exists
echo "✅ Verifying .env.example..."
if [ ! -f ".env.example" ]; then
    echo "❌ Error: .env.example not found!"
    exit 1
fi

# Run verification script
echo ""
echo "🔍 Running verification..."
if [ -f "scripts/verify-clean.sh" ]; then
    bash scripts/verify-clean.sh
else
    echo "⚠️  Verification script not found, skipping..."
fi

echo ""
echo "✅ Release preparation complete!"
echo ""
echo "Next steps:"
echo "1. Review changes: git status"
echo "2. Commit changes: git add . && git commit -m 'chore: prepare release'"
echo "3. Tag release: git tag -a v1.0.0 -m 'Release v1.0.0'"
echo "4. Push to GitHub: git push origin main --tags"
