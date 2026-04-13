# -*- coding: utf-8 -*-
import argparse
import re
from rich.console import Console
from rich.text import Text
from rich_argparse import RichHelpFormatter

class CustomHelpFormatter(RichHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """自定义帮助格式化器，结合Rich的色彩和自定义的简洁格式
    修正：保留 description/epilog 中的显式换行符，避免被自动重排为单行
    """
    
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

    # 修正：覆盖 _fill_text，强制保留原文本与换行，避免Rich/argparse自动换行导致的行合并
    def _fill_text(self, text, width, indent):
        """
        保留传入文本的原始结构（含换行与缩进），不进行自动重排。
        用于确保 epilog/description 中的多行示例在终端正确换行显示。
        """
        return text

    # 说明：不覆盖 add_text，避免与 rich_argparse 的内部渲染逻辑冲突

def add_rich_epilog(parser, epilog_rich):
    """
    为 parser 添加一个支持 Rich 渲染和保留换行的 epilog。
    使用 Hook 方式在原 help 打印后追加 epilog。
    """
    _orig_print_help = parser.print_help
    def _print_help_with_epilog(file=None):
        _orig_print_help(file=file)
        try:
            from rich.console import Console
            console = Console(force_terminal=True)
            console.print(epilog_rich)
        except Exception:
            # 发生异常时，降级为纯文本打印
            print(epilog_rich)
    parser.print_help = _print_help_with_epilog

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
