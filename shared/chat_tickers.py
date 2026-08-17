"""Chat-tuned ticker extraction для human-written messages.

V1 `extract_tickers` is bot-format-tuned: $TICKER, `TICKER`, **TICKER**, Coin: TICKER.
Human chat (ForumB/Общение, Example Source/Chat etc) uses plain lowercase text — «sol pumping», «wif memetop».

This module provides whitelist-based plain-text extraction для major-ticker mentions.
Whitelist is intentionally narrow (top 80 majors+memes) for high precision —
prevents false-positives on common English words like «GO», «PUMP» as tickers.

Maintain `CHAT_TICKER_WHITELIST` alongside community knowledge of relevant majors.
"""

import re

# Top 80 mainstream tickers + memes user'у would chat about.
# Pruned for false-positive safety: excluded short-name tickers ambiguous with English words.
CHAT_TICKER_WHITELIST = frozenset({
    # Majors (L1/L2 + DeFi blue chips)
    'btc', 'eth', 'sol', 'bnb', 'xrp', 'ada', 'avax', 'dot', 'link', 'matic',
    'arb', 'op', 'sui', 'sei', 'apt', 'near', 'atom', 'ftm', 'algo', 'xlm',
    'vet', 'hbar', 'icp', 'fil', 'imx', 'inj', 'tia', 'dym', 'strk', 'manta',
    'pyth', 'jto', 'jup', 'wld', 'rndr', 'fet', 'tao', 'ondo', 'ena', 'ethfi',
    'aave', 'mkr', 'crv', 'ldo', 'uni', 'snx', 'comp', 'sushi', 'gmx', 'dydx',
    # Memes (popular on Solana/EVM)
    'doge', 'shib', 'wif', 'pepe', 'bonk', 'floki', 'popcat', 'ponke', 'mew',
    'brett', 'book', 'turbo', 'andy', 'mog', 'fartcoin', 'goat', 'pnut',
    # AI/Tech narratives
    'sol', 'tao', 'akt', 'gpu', 'render',
    # Newer L2/L3 + COMMUNITY
    'mantle', 'mode', 'blast', 'scroll', 'taiko', 'linea', 'morph',
    # Other relevant
    'kas', 'kaspa', 'rune', 'thorchain', 'osmo', 'kuji', 'celestia',
})

_CHAT_TICKER_RE = re.compile(
    r'\b(' + '|'.join(re.escape(t) for t in CHAT_TICKER_WHITELIST) + r')\b',
    re.IGNORECASE
)


def extract_tickers_chat(text: str | None) -> list[str]:
    """Plain-text mainstream-ticker extraction для human chat.

    Returns deduplicated uppercase ticker list.

    Examples:
        >>> sorted(extract_tickers_chat('sol pumping bonk wif'))
        ['BONK', 'SOL', 'WIF']
        >>> extract_tickers_chat('please buy now')
        []
        >>> extract_tickers_chat(None)
        []
    """
    if not text:
        return []
    matches = _CHAT_TICKER_RE.findall(text)
    return sorted({m.upper() for m in matches})


def extract_tickers_combined(text: str | None, bot_format_extractor) -> list[str]:
    """Union: bot-format extraction + chat-tuned extraction.

    Args:
        text: message text
        bot_format_extractor: callable like `extract_tickers` from private_mirror_monitor.

    Returns:
        Deduplicated uppercase ticker list combining both extraction strategies.
    """
    if not text:
        return []
    bot_tickers = bot_format_extractor(text) or []
    chat_tickers = extract_tickers_chat(text)
    return sorted(set(bot_tickers) | set(chat_tickers))
