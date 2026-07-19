# GMGN Wallet Intelligence Terminal

Bu repo, Pump.fun kökenli başarılı tokenların erken alıcılarını tespit edip GMGN verileriyle ATH ve realized PnL kriterlerine göre filtreleyen araştırma hattıdır.

## Aktif araştırma fazı

**Faz 1:** 1 Ocak 2026–18 Temmuz 2026

Sonraki fazlar:

1. 2025
2. 19 Ocak–31 Aralık 2024

Y tokenının aktif faz içinde veya Pump.fun üzerinde oluşturulmuş olması gerekmez.

## Sabit kriterler

- X tokenı aktif faz içinde Pump.fun üzerinde oluşturulmuş olmalı.
- X tokenı en az **$10.000.000 ATH market cap** görmüş olmalı.
- Wallet, X tokenını gerçek swap ile **$50.000 market cap altında** almış olmalı.
- Wallet, X tokenında en az **25x realized multiple** sağlamış olmalı.
- Aynı walletın X’ten farklı bir Y tokenında en az **$75.000 realized net PnL** değeri bulunmalı.
- Transfer ve airdrop buy sayılmaz.
- Unrealized kâr realized kâr sayılmaz.
- Eksik veri başarılı sonuç kabul edilmez.

Kriterler `config/research_criteria.json` dosyasında kayıtlıdır.

## Veri kaynakları

- **BigQuery:** Pump.fun create, migration ve hedef tokenların gerçek buy işlemleri
- **GMGN:** ATH market cap, wallet activity, holdings ve realized PnL
- **Supabase:** Ham cevaplar, adaylar ve finalist sonuçların saklanması

## Güncel çalışma sırası

1. GMGN ATH yöntemi küçük probe ile doğrulanır.
2. Ocak–Temmuz 2026 Pump.fun discovery taraması çalıştırılır.
3. Yedi aylık sonuç tek listede birleştirilir.
4. ATH ≥ $10M tokenlar seçilir.
5. Bu tokenların $50K altındaki gerçek alıcıları bulunur.
6. Wallet geçmişi cursor bitene kadar çekilir.
7. X 25x ve farklı Y $75K kriterleri hesaplanır.
8. Finalistler zincir işlemleriyle doğrulanır.
9. Sonuçlar Supabase ve terminale yazılır.

## Güncel workflowlar

```text
.github/workflows/gmgn-ath-capability-probe.yml
.github/workflows/wallet-intelligence-2026-discovery-v5.yml
