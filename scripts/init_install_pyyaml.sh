#!/bin/bash
# Install PyYAML on Databricks cluster startup
/databricks/python/bin/pip install PyYAML==6.0.2 2>&1 | tee /tmp/pyyaml_install.log
echo "PyYAML installation complete"
