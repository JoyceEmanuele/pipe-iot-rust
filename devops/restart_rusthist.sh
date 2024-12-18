#!/bin/bash

./rusthist --test-config || exit 1

sudo systemctl restart dap-rusthist.service
