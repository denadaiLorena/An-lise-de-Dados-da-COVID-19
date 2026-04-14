# Painel COVID-19 — Espírito Santo (Streamlit)

Dashboard interativo em **Streamlit** para análise de microdados de COVID-19 do ES, com foco em:

- **Performance** (pré-processamento em Parquet, cache e carregamento rápido)
- **Filtros simples e objetivos** (município e período)
- **Indicadores com regra de negócio explícita** (ex.: letalidade calculada por **desfechos finais**)
- **Visualizações** (Plotly) e narrativa (“data storytelling”) entre gráficos
- **Exportação** do recorte filtrado em **CSV compactado** (`.csv.gz`)

> O app **lê o dataset a partir do arquivo** `dados_es_filtrados.parquet`.
> Se ele não existir, o projeto pode **gerar esse Parquet** a partir de um CSV (`MICRODADOS.csv`) ou baixar uma base via `PARQUET_URL`/`MICRODADOS_URL` (para deploy no Streamlit Community Cloud).

---

## Link do projeto:
**https://an-lise-de-dados-da-covid-19-epdcsgchyceswrkjnqhd3r.streamlit.app/**

## 1) Tecnologias

- Python
- Streamlit
- Pandas
- NumPy
- PyArrow (Parquet)
- Plotly
- Requests (download opcional no Cloud)

Dependências em [requirements.txt](requirements.txt).

---

## 2) Como executar (Windows)

### 2.1) Criar e ativar ambiente virtual

No PowerShell, dentro da pasta do projeto:

```powershell
python -m venv .venv
```

```powershell
.\.venv\Scripts\Activate.ps1
```
Se o PowerShell bloquear a ativação por policy, rode (apenas na sessão atual) e tente de novo:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### 2.2) Instalar dependências

```powershell
python -m pip install -r requirements.txt
```

### 2.3) Rodar o app

```powershell
python -m streamlit run app.py
```

Se você tiver mais de um Python instalado, prefira rodar usando o executável da venv:

```powershell
.\.venv\Scripts\streamlit.exe run app.py
```

Na primeira execução (se não existir o Parquet), o app tenta preparar a base automaticamente.

---

## 3) Dados: arquivos e pipeline

### Arquivos principais

- `dados_es_filtrados.parquet`
  - **Fonte principal** do app (formato colunar otimizado).
- `MICRODADOS.csv` (opcional)
  - Usado apenas para **gerar** o Parquet na primeira execução (separador `;`, encoding `latin1`).

### Como o Parquet é gerado

A geração é feita por [prepare_data.py](prepare_data.py):

- Lê o CSV **em chunks** (100k linhas)
- Mantém apenas um conjunto de colunas essenciais
- Normaliza comorbidades para `int8` (1 = sim, 0 = não, -1 = desconhecido)
- Cria `Obito` a partir da coluna `Evolucao`
- Salva em Parquet com compressão (snappy)

Você pode rodar manualmente:

```powershell
python prepare_data.py
```

Ou com parâmetros:

```powershell
python prepare_data.py --input MICRODADOS.csv --output dados_es_filtrados.parquet
```

### Carregamento no app

O carregamento e “enriquecimento” do dataset é feito em [covid_app/data.py](covid_app/data.py) pela função `carregar_dados_es()`:

- Se o Parquet **não existe**, o app chama `prepare_data.py` e mostra progresso.
- Ao ler o Parquet, o app:
  - padroniza `Evolucao` (tratando `-`/nulos)
  - remove inconsistências (ex.: evolução indica óbito, mas `DataObito` é nula)
  - cria `Status_Analise` de forma vetorizada (rápido) usando `np.select`

---

## 4) Estrutura do projeto (arquivos)

- [app.py](app.py)
  - Entrypoint do Streamlit.
  - Aplica CSS global (accent da UI, sidebar, botões, etc.).
  - Orquestra a renderização das seções.

- [covid_app/](covid_app/)
  - Pacote principal com os módulos organizados por responsabilidade.

- [covid_app/data.py](covid_app/data.py)
  - Carregamento do Parquet.
  - Regras de saneamento.
  - Criação de `Status_Analise`.

- [prepare_data.py](prepare_data.py)
  - Conversão `MICRODADOS.csv` → `dados_es_filtrados.parquet`.

- [covid_app/filters.py](covid_app/filters.py)
  - Filtros na sidebar (município + período) e aplicação do recorte.

- [covid_app/sections/](covid_app/sections/)
  - Seções do dashboard (KPIs, tabela, mapa/ranking, comorbidades/faixa etária, sobrevida, temporal, cura, metodologia).

- [covid_app/export.py](covid_app/export.py)
  - Exportação do recorte filtrado via `st.download_button`.

- [covid_app/geo.py](covid_app/geo.py)
  - Carregamento do GeoJSON e normalização de nomes de município (para casar com o mapa).

- [covid_app/features.py](covid_app/features.py)
  - Funções auxiliares (ex.: extração de idade em anos).

- [covid_app/nav.py](covid_app/nav.py)
  - Âncoras e itens do sumário (navegação por seção).

---

## 5) Filtros

Os filtros ficam na sidebar:

- **Município**: `Todos` ou um município específico.
- **Período**: intervalo entre a menor e maior data detectada.

O app tenta descobrir automaticamente qual coluna de data usar, priorizando:

1. `DataNotificacao`
2. `DataDiagnostico`
3. `DataCadastro`
4. `DataColeta`
5. `DataInicioSintomas`

---

## 6) Indicadores e regras de negócio

### 6.1) `Status_Analise`

O app cria a coluna `Status_Analise` a partir de `Evolucao` e presença/ausência de `DataObito`:

- **Óbito**: `Evolucao` contém “bito”
- **Recuperado**: `Evolucao` contém “Cura”
- **Em Aberto / Ignorado**: `DataObito` é nula (sem desfecho final registrado)
- **Outros**: restante

### 6.2) Letalidade (regra importante)

A **taxa de letalidade** exibida no app é calculada usando **apenas desfechos finais**:

$$\text{Letalidade} = \frac{\text{Óbitos}}{\text{Óbitos} + \text{Recuperados}} \times 100$$

Isso evita distorções quando há muitos casos **em aberto/ignorados** no recorte.

> Observação: em algumas tabelas, pode existir um campo de “Casos”/“Notificações” (volume) que usa o total do recorte — esse volume é útil para tamanho amostral, mas a **letalidade** sempre deve ser interpretada pelos **desfechos**.

---

## 7) Visualizações

O app usa Plotly para:

- Tabela por município (com estilo condicional para volume)
- Mapa coroplético por município (volume de notificações e hover com desfechos/letalidade)
- Ranking Top 10 de risco (letalidade, com corte de volume mínimo)
- Gráficos por comorbidades (volume e risco)
- Perfil por faixa etária (volume e risco)
- Análise de sobrevida (dias da notificação ao óbito) com tratamento de outliers (IQR)
- Evolução temporal da letalidade
- Análises de cura (recuperados) + mapa de taxa de cura

---

## 8) Exportação

A exportação é implementada em [covid_app/export.py](covid_app/export.py):

- Baixa **exatamente o recorte filtrado** (município e período)
- Formato: `CSV` compactado (`.csv.gz`)
- Há um limite de segurança (padrão `300_000` linhas) para não travar a interface

---

## 9) UI / Tema

O app aplica CSS global em [app.py](app.py), com as seguintes regras:

- **Azul `#0047AB` como accent** para títulos, botões e ícones.
- Botão de exportação com estilo **neutro/cinza**.
- Sidebar com fundo ajustado para ser confortável também no **Dark Mode**.

Se você quiser alterar cores, procure pelo bloco `<style>` dentro de `render_sidebar_sumario()` em [app.py](app.py).

---

### Configuração

- **Main file path**: `app.py`

---

## 10) Dicas e troubleshooting

### “Não aparece nada / sem dados”

- Verifique se o Parquet `dados_es_filtrados.parquet` está presente.
- Confira se os filtros (município/período) não estão restringindo demais.

### “Primeira execução demora”

É esperado: o app pode estar lendo o Parquet pela primeira vez. Depois disso, o carregamento fica muito mais rápido.

### “Quero forçar recriar o Parquet”

- Apague `dados_es_filtrados.parquet` e rode novamente o app, **ou** execute:

```powershell
python prepare_data.py
```

---


