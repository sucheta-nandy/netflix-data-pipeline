#!/bin/bash

# Netflix Data Pipeline - GitHub Deployment Script

echo "🎬 Netflix Data Engineering Pipeline - GitHub Deployment"
echo "=========================================================="
echo ""

# Check if git is configured
if ! git config user.name > /dev/null 2>&1; then
    echo "⚠️  Git user name not configured."
    echo "Please run:"
    echo "  git config --global user.name \"Your Name\""
    echo "  git config --global user.email \"your.email@example.com\""
    echo ""
    read -p "Press Enter after configuring Git..."
fi

echo "📝 Step 1: Create GitHub Repository"
echo "--------------------------------------"
echo "1. Go to: https://github.com/new"
echo "2. Repository name: netflix-data-pipeline (or your choice)"
echo "3. Description: Production-grade ETL pipeline with Apache Spark and automated validation"
echo "4. Make it PUBLIC"
echo "5. DO NOT initialize with README"
echo "6. Click 'Create repository'"
echo ""
read -p "Press Enter when you've created the repository..."

echo ""
echo "📋 Step 2: Enter Your GitHub Username"
echo "--------------------------------------"
read -p "GitHub Username: " GITHUB_USERNAME

if [ -z "$GITHUB_USERNAME" ]; then
    echo "❌ Username cannot be empty!"
    exit 1
fi

echo ""
echo "🔗 Step 3: Connecting to GitHub"
echo "--------------------------------------"

# Add remote
REPO_URL="https://github.com/$GITHUB_USERNAME/netflix-data-pipeline.git"
echo "Adding remote: $REPO_URL"

if git remote get-url origin > /dev/null 2>&1; then
    echo "Remote 'origin' already exists. Removing it..."
    git remote remove origin
fi

git remote add origin "$REPO_URL"

echo ""
echo "🚀 Step 4: Pushing to GitHub"
echo "--------------------------------------"

# Rename branch to main if needed
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "Renaming branch to 'main'..."
    git branch -M main
fi

echo "Pushing to GitHub..."
echo ""
echo "⚠️  You may be prompted for your GitHub credentials."
echo "    If you have 2FA enabled, you'll need a Personal Access Token."
echo "    Create one at: https://github.com/settings/tokens"
echo ""

git push -u origin main

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Successfully pushed to GitHub!"
    echo ""
    echo "🌐 Your repository is now live at:"
    echo "   https://github.com/$GITHUB_USERNAME/netflix-data-pipeline"
    echo ""
    echo "📊 Use this URL for your Netflix application:"
    echo "   https://github.com/$GITHUB_USERNAME/netflix-data-pipeline"
    echo ""
    echo "🎉 Deployment Complete!"
else
    echo ""
    echo "❌ Push failed. Please check your credentials and try again."
    echo ""
    echo "Troubleshooting:"
    echo "- Make sure you created the repository on GitHub"
    echo "- Check your GitHub username is correct"
    echo "- If you have 2FA, use a Personal Access Token instead of password"
    echo "  Create one at: https://github.com/settings/tokens"
fi
