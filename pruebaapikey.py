# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 20:07:43 2024

@author: Administrador
"""
import os

api_key = os.getenv("OPENAI_API_KEY")

if api_key:
    print("API key is set correctly.")
else:
    print("API key is not set.")

