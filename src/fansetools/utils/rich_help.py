# -*- coding: utf-8 -*-
import argparse
import re
from rich.console import Console
from rich.text import Text
from rich_argparse import RichHelpFormatter

class CustomHelpFormatter(RichHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """自定义帮助格式化器，结合Rich的色彩和自定义的简洁格式"""
    
    def _format_action(self, action):
        # 简化子命令的显示格式
        if isinstance(action, argparse._SubParsersAction):
            # 只显示子命令名称和简短描述
            parts = []
            # 使用Rich的样式
            # 创建临时Console用于渲染ANSI代码
            c = Console(force_terminal=True)
            for choice, subparser in action.choices.items():
                # 获取子命令的简短描述
                help_text = subparser.description if subparser.description else ""
                # 手动添加一些颜色代码并渲染为ANSI
                with c.capture() as capture:
                    # 修正：移除固定宽度，让rich自动处理换行
                    c.print(f"  [bold cyan]{choice}[/bold cyan] {help_text}", end='')
                parts.append(capture.get())
            
            return "\n".join(parts) + "\n"
        return super()._format_action(action)

def print_colored_text(text):
    """
    Colorize and print raw help text from tools like fastp/cutadapt.
    为 fastp/cutadapt 等工具的原生帮助文本添加颜色
    """
    console = Console(force_terminal=True)
    lines = text.splitlines()
    for line in lines:
        # Simple heuristic styling
        rich_line = Text(line)
        
        # Highlight flags (e.g. -a, --adapter)
        # Regex: optional whitespace, then -x or --long-flag
        flags = list(re.finditer(r'(?<!\w)(-[a-zA-Z0-9?](?=\s|[,=]|$)|--[a-zA-Z0-9_-]+)', line))
        for match in flags:
            rich_line.stylize("bold cyan", match.start(), match.end())
        
        # Highlight default values (e.g. [=auto])
        defaults = list(re.finditer(r'\[=[^\]]+\]', line))
        for match in defaults:
            rich_line.stylize("dim yellow", match.start(), match.end())
        
        # Highlight types (int, string)
        types = list(re.finditer(r'\((int|string|long)\)', line))
        for match in types:
            rich_line.stylize("italic blue", match.start(), match.end())
        
        console.print(rich_line)
