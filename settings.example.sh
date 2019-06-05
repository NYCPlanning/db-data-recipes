#!/bin/bash
## install custom python pacakges
pip3 install -e .

## settings for cli
eval "$(_COOK_COMPLETE=source cook)"
