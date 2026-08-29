# Use Cases — streaming-project

This document holds the perspective of the platform's **users** — what someone builds
*with* streaming-project, and why they care. It is deliberately separate from
[VISION.md](VISION.md), which describes what the platform itself is.

---

# Conviction Index

## The problem

A price movement does not tell you whether anything is behind it.

Bitcoin's price can rise because people are genuinely buying and settling transactions
on-chain — demand that costs something and leaves a trace. It can also rise because
leveraged positions are pushing it, on a network that is sitting idle, with nobody
actually moving coins. The chart looks identical in both cases. The two situations have
opposite implications for what happens next.

Nothing in a price feed distinguishes them, because the distinguishing evidence is not
in the price. It is in two places the price does not look: the state of the network
underneath, and the state of leverage on top.

## The idea

Cross three signals over the same stretch of time:

| Signal | The question it answers |
| --- | --- |
| Spot price, across multiple exchanges | Is the price actually moving, and do the venues agree? |
| Bitcoin mempool congestion | Is the network backing the move — are people competing for block space? |
| Futures leverage | Is the move being pushed by borrowed positions rather than spot demand? |

The point is not three metrics side by side on a dashboard. **The same price change means
something different depending on what the other two are doing at that moment:**

- price up **+** network congested **+** leverage neutral → plausibly organic demand
- price up **+** network idle **+** leverage stretched → thin conviction, leverage-driven

That is a genuine cross, not three panels. The signals have to be brought together
before the verdict is formed, not after.

## The awkward part, which is the interesting part

These three signals do not arrive at anything like the same speed.

Trades on a major exchange arrive dozens to hundreds of times per second. Network
congestion is meaningful roughly twice a minute. Leverage state moves slower still. The
fastest and slowest signals in this cross are **four orders of magnitude apart**.

This is not an inconvenience to be smoothed over. It is the substance of the problem:
combining a firehose with a slow drip, and producing a verdict that is honest about
both, is the hard part of the idea and the reason it is worth building.

---

## What the user wants to know

Three questions, each producing something the raw signals cannot answer on their own.

### 1. Was this move bought, or was it borrowed?

**Leverage Absorption Ratio.** For every dollar of spot volume actually traded, how much
*new leveraged position* appeared alongside it?

A high ratio means the price is climbing on positions that were opened on credit rather
than on buyers who paid. That is the Conviction Index's core thesis expressed as a
continuous number instead of a binary label — useful because "somewhat leverage-driven"
is a real state, and a red/green verdict cannot express it.

This is the signal that separates a rally from a squeeze while the squeeze is still
happening, rather than after it unwinds.

### 2. How much should I trust this verdict *right now*?

**Conviction Confidence.** A classification is only as fresh as the evidence underneath
it. If the network reading it relies on is four minutes old, the verdict is standing on
old ground — and today nothing in the output would say so. It would look exactly as
confident as one built on evidence from two seconds ago.

This question produces a confidence value alongside every verdict, which decays as the
slow evidence ages, and which collapses when a source stops reporting altogether.

It is the most important of the three, and the least obviously financial. It is a
**data-quality measure over a live pipeline** — the kind of thing that matters to whoever
operates the system, not only to whoever reads its output. It is also the natural source
of the first alert worth receiving: *the verdict you are looking at is no longer
trustworthy, and here is why.*

### 3. What actually happened during *that* move?

**Impulse Sessions.** A fixed five-minute window is an arbitrary unit. Markets do not
move in neat intervals — they move in bursts, separated by quiet. A burst is the natural
unit of "a move", and its length is whatever it turns out to be.

This question treats each burst of trading as a single event with its own boundaries,
and asks what the network and leverage state were *during that specific burst*. The
output is a sequence of discrete, individually classified moves rather than an
undifferentiated stream of window verdicts.

It is also the version that is legible at a glance: each burst becomes a marked band
over the price, colored by what it was.

---

## Deferred, with triggers

Three further questions were explored and set aside. They are recorded here so that
picking one up later is a decision with a reason, not a fresh idea.

| Question | Picked up when |
| --- | --- |
| **Does the network lead or follow the price?** Whether congestion rises before a move (positioning) or after it (reaction) — the same measurement, opposite meaning. | There is enough accumulated history to test whether the correlation exists at all. Building the measurement before knowing there is a signal is speculation. |
| **Do the exchanges disagree?** Sustained price divergence between venues as a sign of thin liquidity or a venue-specific event. | More than one asset is tracked, or a venue anomaly is actually observed and goes unexplained. |
| **Which feed went quiet?** Absence of events as a signal in itself — one venue silent while another is busy. | A source goes silent in practice and it is not noticed promptly. The problem should be felt before the detector is built. |

---

## Why this use case, honestly

This is a learning project, and the ordering was not the one a product would have had:
**the list of technologies came first, and the problem came second.** Pretending
otherwise would be the easiest thing to do in a document like this, and worth avoiding.

The Conviction Index was chosen because it stresses the platform harder than anything
built for it so far — several sources instead of one, two different transport models
instead of one, signals whose arrival rates differ by four orders of magnitude, sources
that have no actor or owner at all, and a genuine cross-source join rather than a count.
Every one of those is a claim the platform makes about itself, put under load.

But the fit should be stated accurately in both directions. Kafka and Flink are required
by the platform regardless of what runs on it. **OpenSearch, Grafana, Kubernetes and
ArgoCD are not technically mandatory for this use case.** It creates the *opportunity*
for them, but does not pretend the need was born on its own.

What the three questions above do is make that opportunity defensible rather than
decorative:

- Asking *why* a verdict came out the way it did means retrieving individual
  observations out of an enormous, heterogeneous pile — which is what a search store is
  for, and what a fixed table is bad at.
- Wanting to be told when confidence collapses, rather than noticing it on a screen,
  is what alerting is for.
- Adding a sixth exchange without disturbing the five already running is what the
  deployment story described in [VISION.md](VISION.md) is for.

Each of those is a want a real user of this system would have. That the answer to each
happens to be a technology worth learning is the fortunate part, and it is fortunate,
not inevitable.

---

## What this use case demands of the platform

Stated as requirements on the platform, without prescribing how they are met:

- Ingest from sources that **push** data continuously, not only sources that can be
  polled on a schedule.
- Ingest from sources that return **a single reading** rather than a list of events, and
  that carry **no identifier and no event type** of their own — a snapshot of a system's
  state is not an event with a name.
- Represent sources that have **no actor** — nobody performs a mempool reading; it simply
  is the case.
- Reconcile the **same concept named differently by each source** onto one shared key, so
  that data from different origins can actually be joined.
- Let each source declare **how late its data may arrive** and **how long its silence
  should be tolerated**, because a source that reports twice a minute and a source that
  reports a hundred times a second cannot share one answer to either question.

---

## When this document changes

This is a photograph of what the platform is being pointed at, not a progress log.

It changes when the use case itself changes — the problem is reframed, a question is
added or dropped, or a second use case appears and earns a section of its own.

It does **not** change when one of the questions above is implemented, when a source is
wired up, or when a dashboard is built. Those are tracked elsewhere in the repository.
