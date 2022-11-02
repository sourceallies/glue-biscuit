#! /usr/bin/env bash
/home/glue_user/.local/bin/pip install -r ./requirements.txt
/home/glue_user/.local/bin/pytest $@
