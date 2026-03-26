# 🧠 ARCHITECTURE.md — Как работает система изнутри

## 1. 🗃 Тип базы данных: что мы используем?

Наша база данных — это **Data Warehouse** с элементами **Data Lakehouse**.

| Критерий | Наш проект |
|---|---|
| **Хранилище** | PostgreSQL + расширение `pgvector` |
| **Тип** | Structured + Vector (гибрид) |
| **Назначение** | Аналитика + поиск по смыслу |
| **Схема** | Фиксированная (schema-on-write) |

**Почему не Lake?**
Data Lake хранит сырые данные без схемы (Parquet/JSON в S3). У нас всё строго типизировано.

**Почему не чистый Warehouse?**
Чистый Warehouse — только агрегированные таблицы для аналитики (OLAP-запросы). У нас ещё есть **векторный поиск** — это Lakehouse-паттерн.

**Правильное определение нашей БД:**
> **Оперативный Data Lakehouse** — структурированное хранилище с поддержкой векторного семантического поиска, оптимизированное для задач NLP/AI.

---

## 2. 📐 Схема базы данных

```
fact_articles          — исходные статьи Factcheck.kz
    article_url (PK)   — уникальный URL
    title              — заголовок
    clean_text         — чистый текст
    verdict            — ЖАЛҒАН / РАСТАЛДЫ / ...
    published_at       — дата публикации
    content_hash       — MD5 хэш (для дедупликации)

fact_chunks            — нарезанные фрагменты статей
    chunk_id (PK)
    article_url (FK)   — ссылка на статью
    chunk_text         — текст чанка (~500 символов)
    embedding (vector) — 1536-мерный вектор от OpenAI ← ГЛАВНОЕ!

threads_posts          — посты из Threads
    post_id (PK)
    username           — аккаунт (@tengrinewskz, ...)
    text               — текст поста
    created_at

threads_claims         — извлечённые утверждения из постов
    claim_id (PK)
    post_id (FK)
    claim_text         — конкретное утверждение

matches                — результаты проверки
    claim_id (FK)
    best_article_url   — ссылка на источник
    similarity_score   — косинусное сходство (0..1)
    verdict            — SUPPORTED / REFUTED / NOT_ENOUGH_INFO
    explanation_kk     — объяснение на казахском
    raw_response       — полный JSON от GPT
```

---

## 3. 🔢 Как работает Embedding (векторизация)?

**Embedding** — это превращение текста в массив чисел (вектор), где похожие тексты имеют похожие векторы в многомерном пространстве.

### Шаги:

```
Текст статьи
     ↓
[Нарезка на чанки по ~500 символов с перекрытием 50]
     ↓
Каждый чанк → OpenAI API (model: text-embedding-3-small)
     ↓
Получаем вектор из 1536 чисел: [0.023, -0.145, 0.891, ...]
     ↓
Сохраняем в PostgreSQL (колонка типа vector(1536) через pgvector)
```

### Зачем нарезать на чанки?
- GPT имеет лимит токенов на контекст
- Маленький чанк = точнее находит нужный абзац, а не всю статью целиком
- Перекрытие чанков обеспечивает непрерывность смысла

### Код (файл `api/embedder.py`):
```python
response = openai.embeddings.create(
    model="text-embedding-3-small",
    input=chunk_text
)
vector = response.data[0].embedding  # список из 1536 float
```

---

## 4. 🔍 Как работает векторный поиск?

Когда пользователь вводит утверждение для проверки:

```
Утверждение → OpenAI Embedding → вектор запроса [0.017, -0.13, ...]
                                        ↓
                    Косинусное сходство со всеми чанками в БД
                    (через pgvector оператор <=>)
                                        ↓
                    Топ-5 самых похожих чанков (threshold ≥ 0.5)
                                        ↓
                    Чанки + утверждение → GPT-4o-mini
                                        ↓
              GPT анализирует: "Подтверждают ли эти тексты утверждение?"
                                        ↓
                    verdict + confidence + explanation_kk
```

### SQL-запрос (pgvector):
```sql
SELECT chunk_text, article_url,
       1 - (embedding <=> '[0.017, -0.13, ...]'::vector) AS similarity
FROM fact_chunks
ORDER BY embedding <=> '[0.017, -0.13, ...]'::vector
LIMIT 5
```

Оператор `<=>` — это **косинусное расстояние** (чем меньше, тем похожее).

---

## 5. 🕷 Как парсится Factcheck.kz?

**Файл:** `ingestion/factcheck_scraper.py`

### Стратегия Discovery (поиск URL статей):
```
1. Wayback Machine CDX API (web.archive.org)
   → Запрашиваем список всех архивных URL за последние 2 месяца
   → Если SSL ошибка / нет результата → переходим к шагу 2

2. Прямой скрапинг категорий (новый метод)
   → factcheck.kz/kaz/category/zhanalyq/page/1/
   → factcheck.kz/kaz/category/faktchek/page/1/
   → ... (10 категорий)
   → Пагинация пока не встречается статья старше 2 месяцев

3. RSS / Sitemap fallback
   → Только последние 20 статей (резервный вариант)
```

### Стратегия получения HTML статьи:
```
Для каждого URL статьи:
  1. Попытка через Wayback Machine (архивная копия) — обход Cloudflare
  2. Прямой requests.get() — иногда проходит
  3. Playwright (headless chrome) — если Cloudflare блокирует
```

### Парсинг HTML:
```python
soup = BeautifulSoup(html, "html.parser")
title = soup.find("h1").text
body = soup.find("div", class_="entry-content")
verdict = re.search(r"(Жалған|Расталды|Шындық)", text)
```

### Дедупликация:
```python
content_hash = md5(clean_text)
# Если хэш совпадает с уже сохранённым → пропускаем статью
```

---

## 6. 🧵 Как парсится Threads?

**Файл:** `ingestion/threads_topnews.py`

### Почему НЕ используем официальный API?
- Threads API требует платную подписку Meta for Developers
- RapidAPI `threads-api4`: бесплатный тариф = **50 запросов в месяц** (исчерпали в ходе тестирования)

### Текущий метод: Playwright + Cookie сессия

```
1. Пользователь вставляет свой sessionid куки из браузера → .env
   THREADS_SESSION_ID=your_session_cookie_here

2. Playwright запускает headless Chrome внутри Docker

3. Инжектируем sessionid куки в браузерный контекст
   (Threads думает что это реальный пользователь)

4. Открываем страницы профилей:
   https://www.threads.com/@tengrinewskz
   https://www.threads.com/@informburo.kz
   ...

5. Извлекаем текст постов из DOM:
   div[data-pressable-container="true"]

6. Очищаем: убираем кнопки (лайк, репост), короткие строки
```

### Защита от банов:
- Случайная задержка между аккаунтами (2–4.5 секунды)
- Реалистичный User-Agent (Chrome 123)
- Прогрев сессии (`/` → ждём 1.2с → только потом аккаунты)

---

## 7. ⚙️ Airflow DAGs — подробно

### Текущие DAG файлы:

| Файл | DAG ID | Расписание |
|---|---|---|
| `factcheck_ingest_dag.py` | `factcheck_ingest` | каждый час |
| `threads_ingest_dag.py` | `threads_ingest` | ежедневно |
| `threads_ingest_dag.py` | `threads_nightly_topnews` | каждую ночь в 00:00 |

---

### 📋 DAG: `factcheck_ingest`

**Что делает:**
1. Находит новые статьи на Factcheck.kz за последние 2 месяца (через категории)
2. Скачивает HTML каждой статьи
3. Парсит текст, заголовок, вердикт
4. Нарезает на чанки → создаёт embeddings через OpenAI
5. Сохраняет чанки + векторы в `fact_chunks`

**Идемпотентный:** если статья уже есть и не изменилась (`content_hash` совпадает) — пропускает.

---

### 🧵 DAG: `threads_ingest`

**Что делает:**
1. **Task 1 `collect_and_extract`:** Запускает `threads_collector.py` — собирает посты из Threads, извлекает утверждения через GPT (`/extract_claims`)
2. **Task 2 `auto_verify_claims`:** Берёт все необработанные утверждения из БД, проверяет их через `/check` (RAG), сохраняет вердикты в `matches`

---

### 📰 DAG: `threads_nightly_topnews`

**Зачем остался?**
Мы не удаляли этот DAG — он по-прежнему нужен! Вы сами реализовали `threads_topnews.py` с Playwright + sessionid. Этот DAG каждую ночь:

1. Загружает топ-20 постов из 4 казахских аккаунтов через Playwright
2. Для каждого поста извлекает утверждения
3. Проверяет каждое через RAG (`/check`)
4. Сохраняет результаты в `threads_posts` → `threads_claims` → `matches`

**Отличие от `threads_ingest`:**
- `threads_ingest` — более общий, использует `threads_collector.py`
- `threads_nightly_topnews` — специализированный, использует `threads_topnews.py` с Playwright

Если хотите оставить только один → рекомендую оставить `threads_nightly_topnews` (он работает с вашим sessionid).

---

## 8. 🗺 Общая схема потоков данных

```
factcheck.kz
    ↓  (скрапинг категорий)
fact_articles + fact_chunks (embeddings) ← БД, RAG-индекс
                                ↑
Threads Posts                   |
    ↓  (Playwright + sessionid) |
threads_posts                   |
    ↓  (GPT extract_claims)     |
threads_claims                  |
    ↓  (vector search + GPT) ───┘
matches (verdict + ссылка + объяснение)
    ↓
Streamlit UI → пользователь
```
