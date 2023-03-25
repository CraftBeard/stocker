#!/bin/bash
git add .
git commit -m "update at $(date +%Y-%m-%d_%H:%M:%S)"
git push
