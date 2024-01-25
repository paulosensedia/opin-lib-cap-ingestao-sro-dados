#!/usr/bin/env bash

# This file is the entrypoint of the image
# It must be executed as an unprivileged user

# Fail on error
exec ./scripts/create_quality_profile_and_quality_gate_sonar.bash &
exec ./bin/run.sh
