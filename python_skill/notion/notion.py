#!/usr/bin/python3
# -*- coding: UTF-8 -*-

from pytion import Notion
SOME_TOKEN="secret_zfm2jh7s9oLGLGPDaZikI3SkN1xxV9TtHVbp1trM4nj"
no = Notion(token=SOME_TOKEN)
todoPage=no.pages.get("6113682606d4445485df2434962b3b67")

todoBlocks=no.blocks.get("6113682606d4445485df2434962b3b67")
todoR=todoBlocks.get_block_children_recursive()
print()
