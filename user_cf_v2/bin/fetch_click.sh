#!/bin/bash

chmod +x filter_click.py
awk -F'\t' '$1==34877148{print $0}' | python filter_click.py
