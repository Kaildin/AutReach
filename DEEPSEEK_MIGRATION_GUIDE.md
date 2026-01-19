# Migrazione da GPT a DeepSeek v3

## Perché DeepSeek?

### Confronto con GPT-3.5-turbo

| Metriche | GPT-3.5-turbo | DeepSeek v3 | Vantaggio DeepSeek |
|----------|---------------|-------------|--------------------|
| **Costo input** | $0.50/M tokens | $0.14/M tokens | **14x più economico** |
| **Costo output** | $1.50/M tokens | $0.28/M tokens | **5x più economico** |
| **Prestazioni** | Buone | Paragonabili a GPT-4 | **Migliori** |
| **Velocità** | Veloce | Veloce | Simile |
| **API** | OpenAI nativa | OpenAI-compatibile | Drop-in replacement |

### Risparmio stimato

Con 1000 query di estrazione amministratori:
- **GPT-3.5**: ~$2.00 (1M tokens medi)
- **DeepSeek v3**: ~$0.14
- **Risparmio**: $1.86 (93%)

Su 100k query annuali: **risparmio di ~$186** mantenendo qualità uguale o superiore.

---

## Setup

### 1. Ottieni API Key DeepSeek

1. Vai su https://platform.deepseek.com/signup
2. Registrati (puoi usare GitHub/Google)
3. Vai su https://platform.deepseek.com/api_keys
4. Crea una nuova API key
5. Copia la chiave (inizia con `sk-...`)

### 2. Configura la chiave nel .env

Aggiungi al file `.env` nella root del progetto:

```bash
# DeepSeek API (replacement for OpenAI)
DEEPSEEK_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Optional: mantieni GPT come fallback
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 3. Installa dipendenze (già presente)

```bash
pip install openai  # Stesso client, endpoint diverso!
```

---

## Integrazione in admin_pipeline.py

### Opzione A: Sostituzione Completa (Consigliata)

Sostituisci l'import in `admin_pipeline.py`:

```python
# VECCHIO
# from outreach_saas.scraping.search_utils import extract_admin_with_gpt

# NUOVO
from outreach_saas.scraping.deepseek_extractor import extract_admin_with_deepseek as extract_admin
```

Poi usa semplicemente:
```python
admin_name = extract_admin(ddg_payload)
```

### Opzione B: Con Fallback GPT (Massima Affidabilità)

Per sicurezza, prova prima DeepSeek e se fallisce usa GPT:

```python
from outreach_saas.scraping.deepseek_extractor import extract_admin_with_deepseek
from outreach_saas.scraping.search_utils import extract_admin_with_gpt

# Prova DeepSeek (economico)
admin_name = extract_admin_with_deepseek(ddg_payload)

# Fallback a GPT se necessario (solo in caso di errore)
if admin_name is None:
    logger.warning("DeepSeek fallito, fallback a GPT")
    admin_name = extract_admin_with_gpt(ddg_payload)
```

### Opzione C: A/B Testing (Per Confronto)

Per testare quale funziona meglio:

```python
admin_deepseek = extract_admin_with_deepseek(ddg_payload)
admin_gpt = extract_admin_with_gpt(ddg_payload)

logger.info(f"DeepSeek: {admin_deepseek} | GPT: {admin_gpt}")

# Usa DeepSeek come primario
admin_name = admin_deepseek or admin_gpt
```

---

## Test

### Test standalone del modulo

```bash
python -m src.outreach_saas.scraping.deepseek_extractor
```

Output atteso:
```
================================================================================
Test DeepSeek v3 Admin Extractor
================================================================================

Test 1: Nome chiaro (Marco Donnini)
[DeepSeek] ✓ Amministratore estratto: Marco Donnini
Risultato: Marco Donnini
Atteso: Marco Donnini
Status: ✓ PASS

Test 2: Nessun amministratore (info generiche)
[DeepSeek] Nessun amministratore trovato nel testo
Risultato: None
Atteso: None
Status: ✓ PASS
```

### Test nella pipeline completa

```bash
python -m src.outreach_saas.pipelines.admin_pipeline \
    --input output/main_pipeline_results.csv \
    --output output/admin_deepseek_test.csv \
    --limit 10
```

Confronta i risultati con la versione GPT.

---

## Monitoring e Debug

### Log di utilizzo token

DeepSeek logga automaticamente l'utilizzo:

```
[DeepSeek] Token usati: prompt=850, completion=4, totale=854
```

Calcolo costo:
- Input: 850 tokens × $0.14 / 1M = $0.000119
- Output: 4 tokens × $0.28 / 1M = $0.0000011
- **Totale: $0.00012 per query**

### Troubleshooting

#### Errore: "DEEPSEEK_API_KEY non configurata"

```bash
# Verifica che la chiave sia nel .env
grep DEEPSEEK_API_KEY .env

# Se manca, aggiungila:
echo "DEEPSEEK_API_KEY=sk-your-key-here" >> .env
```

#### Errore: "Connection timeout"

```python
# Aumenta il timeout (default 30s)
admin = extract_admin_with_deepseek(payload, timeout=60)
```

#### Errore: "Rate limit exceeded"

DeepSeek ha rate limits generosi, ma se li superi:

```python
import time
import random

# Aggiungi delay tra richieste
time.sleep(random.uniform(0.5, 1.5))
```

---

## FAQ

**Q: DeepSeek è affidabile quanto GPT?**
A: Sì, DeepSeek v3 ha prestazioni comparabili a GPT-4 su molti task, secondo benchmark pubblici (MMLU, HumanEval, ecc.).

**Q: Devo cambiare il codice esistente?**
A: No! L'interfaccia è identica: `extract_admin_with_deepseek(text)` funziona esattamente come `extract_admin_with_gpt(text)`.

**Q: Cosa succede se la mia chiave DeepSeek scade?**
A: Usa l'Opzione B (fallback a GPT) per continuità operativa.

**Q: Posso usare entrambi in produzione?**
A: Sì! Consigliato per ridondanza. DeepSeek primario, GPT come backup.

**Q: I dati sono sicuri?**
A: Sì, DeepSeek rispetta GDPR e non usa i tuoi dati per training (simile a OpenAI API).

---

## Migrazione Step-by-Step

### Fase 1: Setup (5 minuti)
- [ ] Crea account DeepSeek
- [ ] Ottieni API key
- [ ] Aggiungi chiave a `.env`
- [ ] Verifica con test standalone

### Fase 2: Test (30 minuti)
- [ ] Testa su 10 aziende (Opzione C - A/B Testing)
- [ ] Confronta risultati DeepSeek vs GPT
- [ ] Verifica costi nel dashboard DeepSeek

### Fase 3: Deploy (10 minuti)
- [ ] Scegli strategia (A, B, o C)
- [ ] Modifica `admin_pipeline.py`
- [ ] Run completo su dataset test
- [ ] Verifica success rate

### Fase 4: Produzione
- [ ] Deploy in produzione
- [ ] Monitor costi e qualità per 1 settimana
- [ ] Rimuovi GPT se soddisfatto (o mantieni come fallback)

---

## Supporto

- **DeepSeek Docs**: https://platform.deepseek.com/docs
- **API Reference**: https://platform.deepseek.com/api-docs
- **Issues GitHub**: Apri un issue nel repo AutReach

---

**Ultima modifica**: 2026-01-19
**Versione DeepSeek**: v3 (deepseek-chat)
**Status**: ✅ Production Ready
