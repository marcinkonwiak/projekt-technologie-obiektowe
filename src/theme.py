from textual.theme import Theme

default_theme = Theme(
    name="default",
    primary="#C45AFF",
    secondary="#a684e8",
    accent="#FF69B4",
    foreground="#D8DEE9",
    background="#000000",
    success="#A3BE8C",
    warning="#EBCB8B",
    error="#BF616A",
    surface="#3B4252",
    panel="#434C5E",
    dark=True,
    variables={
        "block-cursor-text-style": "none",
        "footer-key-foreground": "#88C0D0",
        "input-selection-background": "#81a1c1 35%",
    },
)
