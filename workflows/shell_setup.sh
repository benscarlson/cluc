# This file should be sourced

set -u # Throw an error when trying to access non-existant variables

# These only run in zsh
if [ -n "$ZSH_VERSION" ]; then
  # Running in zsh
  setopt interactivecomments
  bindkey -e
fi

