# GMGN Wallet Intelligence Terminal

Bu repo, Pump.fun kökenli erken alıcıları **tam BigQuery tarih evreninden** çıkarıp GMGN ile ATH ve realized PnL açısından süzen bir araştırma hattıdır.

## Sabit kriterler

- Pump.fun kökeni, başlangıç: **19 Ocak 2024**
- Token ATH market cap: **en az $10.000.000**
- Wallet'ın gerçek buy anındaki market cap: **$50.000 altında**
- X tokenında gerçekleşmiş satış maliyet bazına göre: **en az 25x**
- Aynı wallet'ın X'ten farklı Y tokenında realized net PnL'i: **en az $75.000**
- Transfer/airdrop buy sayılmaz

Kriterler `config/research_criteria.json` içinde de kayıtlıdır.

## Veri sorumlulukları

- **BigQuery:** limitsiz Pump.fun create/buy/sell/migrate evreni ve gerçek ilk buy transactionı.
- **GMGN created-tokens:** yalnızca migration görmüş mintler için ATH market cap.
- **GMGN holdings/activity:** realized PnL, X multiple, farklı Y tokenı ve buy anındaki fiyat/supply.
- **Eksiksizlik kapısı:** GMGN'nin 100 tokenlık creator dizisi nedeniyle mint bulunamazsa sonuç sessizce atılmaz; `unresolved_*.csv` oluşur ve workflow kırmızı biter.

## GitHub Secrets

Repository → Settings → Secrets and variables → Actions:

- `GCP_SA_KEY`
- `GMGN_API_KEY`
- `SUPABASE_URL` ve `SUPABASE_SECRET_KEY` yalnız Supabase yüklemesi kullanılırsa gerekir.

Secret dosyalarını repoya yüklemeyin.

## Çalıştırma

1. Actions → **Wallet Intelligence Pipeline**
2. İlk test için `2024-01-19`–`2024-01-25`
3. Test yeşil olduğunda daha geniş tarih aralığı çalıştırın.
4. Artifact: `wallet-intelligence-<start>-<end>`

Önemli çıktılar:

- `artifacts/backfill/pumpfun_eligible_first_buys_*.csv`
- `artifacts/ath/eligible_ath_tokens.csv`
- `artifacts/ath/unresolved_ath_tokens.csv`
- `artifacts/candidates/wallet_finalists.csv`
- `artifacts/candidates/unresolved_wallet_tokens.csv`

## Tamlık davranışı

Hattın temel kuralı: eksik veri, boş sonuç gibi gösterilmez.

- Bilinmeyen Pump instruction → workflow başarısız
- Eksik gün → workflow başarısız
- Creator token listesinde hedef mint bulunamazsa → ATH unresolved
- BigQuery ve GMGN ilk buy transactionı uyuşmazsa → wallet/token unresolved
- Fiyat veya supply yoksa → market cap tahmin edilmez

## Realized multiple tanımı

GMGN alanlarından:

`realized_cost_basis = history_sold_income - realized_profit`

`realized_multiple = history_sold_income / realized_cost_basis`

Bu tanım yalnız gerçekleşmiş satış maliyet bazını kullanır. Son finalistlerde zincir muhasebesiyle ikinci doğrulama yapılması gerekir; `wallet_finalists.csv` bir güçlü aday listesidir, nihai hukuki/muhasebesel kanıt değildir.

## Yerel test

```bash
python -m pip install -r requirements.txt
pytest -q
python -m compileall -q scripts tests
```

## Eski Dune workflow'u

`.github/workflows/pumpfun-data-foundation.yml` ücretsiz Dune planındaki performance-tier engeli nedeniyle kullanılmıyor. Yeni ana hat BigQuery workflow'larıdır.
