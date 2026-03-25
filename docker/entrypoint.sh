#!/bin/sh

# working directory: project root

# if in demo mode, download the demo assets (if not present)
if [ "$WITH_DEMO_ASSETS" = "true" ]; then
  ./download_demo_assets.sh
fi

# run the launcher (replace this shell, pass all arguments)
python -m tram_analytics.v1.launcher_joint "$@"
