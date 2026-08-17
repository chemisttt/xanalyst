# CT Alpha Digest — Anti-pattern Few-shots

**STATUS: POPULATED 2026-06-09.** Concrete few-shots, grounded в реальном NFT-CT
словаре. Раньше был STUB с TODO-плейсхолдерами — классификатор 19 дней получал
нерабочие примеры (verify +19d). Refresh cadence: **quarterly** (NFT meta drift'ит).
Update: вытащить 4 свежих кейса того же типа из последних 2 недель X-фида.

Каждый паттерн — это **частая ошибка классификации**, которую мы явно гасим. Формат:
входной твит → правильное решение → типичная ошибка (NOT).

---

## Pattern 1: post-mint ≠ filter. Завершённый минт = `state_reconcile`, не reject.

> Tweet: "@bobosoneth minted out — 4444/4444 gone in 38 min, secondary opening on
> @opensea now, floor 0.018 / mint was 0.008. gm to holders 🫡"

- **bucket=`state_reconcile`**, included=true, tags=["post-mint","sold-out"].
- novelty_score ≈ 0.15 (это апдейт статуса, не новая механика).
- mechanic_notes: "Сейллаут 4444 за 38 мин, минт 0.008 ETH, вторичка открылась, флор 0.018."
- **NOT:** included=false «минт уже прошёл». Завершённость минта — это сигнал
  (sellout speed, флор vs mint price), а не повод выкинуть.

---

## Pattern 2: anon + новый примитив = `early_signals` с высокой novelty. Не фильтруй за low followers.

> Tweet: "@0xnobody_eth (anon, ~80 followers) launching one-tx-per-wallet bonding
> curve — 0.001 ETH floor mint, no max supply, duplicate addr auto-refunds. contract
> live, public in ~2h"

- **bucket=`early_signals`**, novelty_score ≥0.75, included=true.
- tags=["bonding-curve","per-wallet-limit","anon"].
- mechanic_notes: "Bonding curve, 0.001 ETH старт, без макс-саплая, авто-рефанд на
  дубликат адреса, лимит 1 tx/кошелёк. Анон-деплой."
- **NOT:** filter за ~80 followers. Novelty механики перевешивает credibility floor —
  именно такие ранние анон-дропы и есть alpha.

---

## Pattern 3: astroturf — детектим и тэгаем, но `included=true`. Не прячем.

> Tweet (один из 5 почти идентичных за 8 мин от 5 разных аккаунтов): "JUST FOUND
> $GHIBLISWAP 👀 stealth launch, 0 tax, LP burned, this is THE one. ape before it's
> too late 🚀🚀 [same link]"

- **bucket=`paid_hype`**, included=true, tags=["astroturf_cluster","coordinated","shill"].
- novelty_score ≈ 0.05.
- mechanic_notes: "Координированный промо-кластер: 5 near-identical постов за 8 мин
  с 5 аккаунтов, один и тот же линк."
- **NOT:** included=false. Мы СУРФЕЙСИМ детект (UI даёт юзеру фильтр) — скрытие лишает
  юзера сигнала «это организованный шилл».

---

## Pattern 4: спорная механика — нейтральный язык. Без морализаторства.

> Tweet: "TwoBit drop: каждый минт уменьшает % выплаты следующего минта в creator pool
> (residue drain). 0.02 ETH, 3333 supply, WL 12:00 UTC завтра."

- **bucket=`calendar_24h`** (есть дата), included=true.
- promised_timestamp_iso = завтрашние 12:00 UTC в ISO8601.
- mechanic_notes: "Residue drain механика — доля creator pool падает с каждым минтом.
  0.02 ETH, supply 3333." (НЕ повторяй дату — она отдельной строкой).
- **NOT:** "exploitative" / "dangerous" / "ponzi" / "scam" / "разводка". Описываем
  механику нейтральным глаголом, юзер решает сам.

---

## Calibration note (2026-06-09): novelty discrimination

Распределение novelty в проде сжато (~0.34 avg в early_signals — verify +19d). Дайджест
ранжирует top-12 ПО novelty, поэтому компрессия скоров = слабый ранкинг. Целься в широкий
спред:
- **0.10–0.25** — стандартный mint-анонс известного формата (WL/public/FCFS без новизны).
- **0.30–0.50** — заметный твист (необычный saplay/price, новый caller-кластер).
- **0.60–0.85** — реально новая механика/примитив (как Pattern 2).
- **>0.85** — редкость: never-seen mechanic.
Не сваливай всё в 0.3–0.4. Если сомневаешься между «обычное» и «новое» — ставь ниже.
