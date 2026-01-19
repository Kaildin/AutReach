#!/usr/bin/env python3
import sys
import os

# Aggiungi la root del progetto al pythonpath
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outreach_saas.pipelines.main_pipeline import main

if __name__ == "__main__":
    main()
