import pandas as pd
import streamlit as st
import numpy as np
import os
import subprocess
import requests
import plotly.express as px

@st.cache_data
def carregar_dados_es():
    """
    Carrega a base consolidada do ES no formato otimizado Parquet.
    Se o arquivo não existir, tenta gerá-lo usando o prepare_data.py.
    """
    file_path = 'dados_es_filtrados.parquet'
    
    if not os.path.exists(file_path):
        # Espaço vazio para descer o aviso
        for _ in range(5):
            st.write("")
            
        st.info("ℹ️ **Aviso:** Como esta é a primeira vez, o processamento inicial levará em média 60 segundos.")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with st.status("Preparando base de dados otimizada...", expanded=True) as status:
            # Executa o script e lê a saída em tempo real
            # Usamos 'latin1' ou 'cp1252' pois no Windows o terminal costuma usar encoding local
            process = subprocess.Popen(
                ["python", "prepare_data.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='latin1',
                errors='replace',
                bufsize=1
            )
            
            if process.stdout:
                for line in process.stdout:
                    if "PROGRESS:" in line:
                        try:
                            # Extrai apenas o número após PROGRESS:
                            parts = line.split("PROGRESS:")
                            if len(parts) > 1:
                                value_str = parts[1].strip().split()[0]
                                percent = int(value_str)
                                prog_val = max(0, min(100, percent))
                                progress_bar.progress(prog_val)
                                status_text.text(f"Progresso: {prog_val}%")
                        except (ValueError, IndexError):
                            continue
            
            process.wait()
            status.update(label="✅ Dados preparados com sucesso!", state="complete", expanded=False)
            status_text.empty()
            progress_bar.empty()

    df = pd.read_parquet(file_path)
    
    # Padronizar a coluna Evolucao removendo hifens residuais ou nulos
    df['Evolucao'] = df['Evolucao'].astype(str).replace({'-': 'Ignorado', 'nan': 'Ignorado'})
    
    # Remover registros com inconsistência: Evolucao indica Óbito, mas DataObito é nula
    obitos_mask = df['Evolucao'].astype(str).str.contains('bito', na=False, case=False)
    inconsistentes = obitos_mask & df['DataObito'].isnull()
    df = df[~inconsistentes]
    
    # ---  MÉTRICA DE OURO:  MUNDO REAL ---
    # Ao invés de df.apply(axis=1) que é lento, usamos np.select para vetorização máxima (leva milissegundos)
    cond_obito = df['Evolucao'].astype(str).str.contains('bito', case=False, na=False)
    cond_cura = df['Evolucao'].astype(str).str.contains('Cura', case=False, na=False)
    cond_aberto = df['DataObito'].isnull()

    df['Status_Analise'] = np.select(
        [cond_obito, cond_cura, cond_aberto],
        ['Óbito', 'Recuperado', 'Em Aberto / Ignorado'],
        default='Outros'
    )
    # Convertendo a Métrica de Ouro para 'category' para manter a otimização de RAM
    df['Status_Analise'] = df['Status_Analise'].astype('category')
    
    return df


@st.cache_data(show_spinner=False)
def carregar_geojson_municipios_es():
    """Baixa e cacheia o GeoJSON dos municípios do ES.

    Tenta primeiro o endpoint oficial de malhas do IBGE. Se falhar (instabilidade/rota),
    usa um fallback público com GeoJSON de municípios por UF.
    """
    # IBGE (malhas): formato GeoJSON
    # Obs: a rota /api/v3/malhas/... não expõe municípios por UF em GeoJSON.
    ibge_url = "https://servicodados.ibge.gov.br/api/v2/malhas/estados/32?formato=application/vnd.geo+json&qualidade=intermediaria"
    try:
        resp = requests.get(ibge_url, timeout=30)
        resp.raise_for_status()
        geo = resp.json()
        # O endpoint acima tende a retornar apenas o estado; portanto, forçamos fallback
        # para ter municípios (necessário para o choropleth por município).
        raise ValueError("IBGE retornou malha do estado (sem municípios)")
    except Exception:
        # Fallback: GeoJSON de municípios do ES (UF=ES)
        # Fonte pública amplamente usada em exemplos (não oficial).
        fallback_url = "https://raw.githubusercontent.com/tbrugz/geodata-br/master/geojson/geojs-32-mun.json"
        resp2 = requests.get(fallback_url, timeout=30)
        resp2.raise_for_status()
        return resp2.json()


def normalizar_municipio(nome: str) -> str:
    if nome is None:
        return ""
    nome = str(nome).strip().upper()
    nome = (
        nome.replace("Á", "A")
            .replace("À", "A")
            .replace("Â", "A")
            .replace("Ã", "A")
            .replace("É", "E")
            .replace("Ê", "E")
            .replace("Í", "I")
            .replace("Ó", "O")
            .replace("Ô", "O")
            .replace("Õ", "O")
            .replace("Ú", "U")
            .replace("Ç", "C")
    )
    return nome


def _get_geojson_municipio_nome(props: dict) -> str:
    if not isinstance(props, dict):
        return ""
    return props.get('nome') or props.get('name') or ""

def main():
    st.set_page_config(page_title="Painel COVID-19 ES", layout="wide")
    st.title("Painel COVID-19 - Espírito Santo")

    with st.spinner("Carregando e processando a base filtrada do ES..."):
        df = carregar_dados_es()
    
    # Toast de sucesso logo após o título
    st.toast(f"✅ Base carregada: {len(df):,} registros".replace(',', '.'), icon='📊')

    # Mostrar o processo KDD num Expander na barra lateral
    with st.sidebar.expander("Processo KDD e Qualidade de Dados", expanded=False):
        st.markdown('''
        **Knowledge Discovery in Databases (KDD)**
        
        Durante a preparação e carga destes dados, aplicamos tratamentos rigorosos de Engenharia de Dados para garantir a qualidade analítica:
        
        1. **Big Data (Parquet)**: O arquivo que originalmente pesava 876MB em CSV foi comprimido e convertido para um formato colunar Parquet (44MB). Isso reduziu o I/O, preservou tipagens em C e entrega tempos de carregamento na casa dos milissegundos.
        2. **Otimização de Memória**: Atributos de texto massivamente repetitivos (*Municípios*, *Comorbidades*, *Evolução*) foram fixados como `category`, poupando GBs de memória RAM do servidor.
        3. **Limpeza de Inconsistências**: Filtrados e removidos 166 registros "fantasmas" onde a evolução indicava óbito, mas o campo de Data do Óbito estava vazio.
        4. **Métrica "Status Honesto"**: Cientes da complexidade (e sujeira) do mundo real, vetomizamos em milissegundos uma classificação em 4 níveis cruzando as colunas `Evolucao` e `DataObito`. Agora podemos agrupar os pacientes diretamente nas cestas absolutas: **Óbito, Recuperado, Em Aberto / Ignorado, e Outros**.
        ''')
    # Filtros (Sidebar)
    # --------------------
    st.sidebar.header("Filtros")

    # Filtro por Município
    if 'Municipio' in df.columns:
        municipios = sorted(
            [m for m in df['Municipio'].dropna().astype(str).unique().tolist() if m.strip()]
        )
        municipios_opcoes = ['Todos'] + municipios
        municipio_sel = st.sidebar.selectbox("Município", municipios_opcoes, index=0)
    else:
        municipio_sel = 'Todos'
        st.sidebar.info("Coluna 'Municipio' não encontrada para filtrar.")

    # Filtro por Período (tenta descobrir a melhor coluna de data)
    date_col_candidates = [
        'DataNotificacao',
        'DataDiagnostico',
        'DataCadastro',
        'DataColeta',
        'DataInicioSintomas',
    ]
    date_col = next((c for c in date_col_candidates if c in df.columns), None)

    if date_col is not None:
        df_dates = pd.to_datetime(df[date_col], errors='coerce')
        min_date = df_dates.min()
        max_date = df_dates.max()

        if pd.notna(min_date) and pd.notna(max_date):
            periodo_sel = st.sidebar.date_input(
                "Período",
                value=(min_date.date(), max_date.date()),
                min_value=min_date.date(),
                max_value=max_date.date(),
            )
        else:
            periodo_sel = None
            st.sidebar.info(f"Não foi possível inferir datas válidas em '{date_col}'.")
    else:
        periodo_sel = None
        st.sidebar.info("Nenhuma coluna de data conhecida foi encontrada para filtrar o período.")

    # Aplicar filtros
    df_filtrado = df

    if municipio_sel != 'Todos' and 'Municipio' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['Municipio'].astype(str) == str(municipio_sel)]

    if date_col is not None and periodo_sel is not None:
        if isinstance(periodo_sel, (tuple, list)) and len(periodo_sel) == 2:
            dt_ini = pd.to_datetime(periodo_sel[0])
            dt_fim = pd.to_datetime(periodo_sel[1])
            dt_series = pd.to_datetime(df_filtrado[date_col], errors='coerce')
            df_filtrado = df_filtrado[(dt_series >= dt_ini) & (dt_series <= dt_fim)]

    # Usar o df já recortado no restante do app
    df = df_filtrado
    
    # --------------------
    # Indicadores Gerais (KPIs) - Mover para o topo
    # --------------------
    st.subheader("Indicadores Gerais de Saúde")
    
    # Calcular métricas principais usando a Métrica Honesta nativa
    total_casos = len(df)
    
    # Identificar óbitos (Pela coluna nativa preparada ou pela Métrica Honesta)
    if 'Obito' in df.columns:
        total_obitos = df['Obito'].astype(int).sum()
    else:
        total_obitos = (df['Status_Analise'] == 'Óbito').sum()
        
    taxa_letalidade = (total_obitos / total_casos) * 100 if total_casos > 0 else 0
    
    # Criar 3 colunas para os KPIs
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total de Casos Confirmados", f"{total_casos:,}".replace(',', '.'))
    kpi2.metric("Total de Óbitos", f"{total_obitos:,}".replace(',', '.'))
    kpi3.metric("Taxa de Letalidade (T.L.)", f"{taxa_letalidade:.2f}%")
    
    st.divider()

    # --------------------
    # Tabela de Desempenho e Microdados - Mover para cima
    # --------------------
    st.subheader("Desempenho por Município (T.L.)")
    st.markdown("A tabela abaixo agrupa dados por município para facilitar a tomada de decisão focada na taxa de letalidade.")
    
    # Agrupar dados por município (usando 'Obito' nativo)
    if 'Obito' in df.columns:
        df_municipio = df.groupby('Municipio').agg(
            Casos=('Municipio', 'count'),
            Obitos=('Obito', lambda x: x.astype(int).sum())
        ).reset_index()
    else:
        df_municipio = df.groupby('Municipio').agg(
            Casos=('Municipio', 'count'),
            Obitos=('Status_Analise', lambda x: (x == 'Óbito').sum())
        ).reset_index()
    
    # Calcular Letalidade
    df_municipio['Taxa Letalidade (%)'] = (df_municipio['Obitos'] / df_municipio['Casos']) * 100
    df_municipio = df_municipio.sort_values(by='Taxa Letalidade (%)', ascending=False)
    
    # Formatação visual sem depender do matplotlib (Usando recursos nativos do Streamlit)
    st.dataframe(
        df_municipio,
        column_config={
            "Casos": st.column_config.NumberColumn(format="%d"),
            "Obitos": st.column_config.NumberColumn(format="%d"),
            "Taxa Letalidade (%)": st.column_config.ProgressColumn(
                "Taxa Letalidade (%)",
                format="%.2f%%",
                min_value=0,
                max_value=max(df_municipio['Taxa Letalidade (%)']) if not df_municipio.empty else 100
            )
        },
        use_container_width=True,
        hide_index=True
    )
    
    with st.expander("🔍 Visualizar Microdados (Amostra de 200 linhas)"):
        st.dataframe(df.head(200), use_container_width=True)

    st.divider()

    # --------------------
    # Mapa (ES): Grande Vitória vs Interior
    # --------------------
    st.subheader("Mapa de Casos por Município (ES)")

    # Definição simples da Grande Vitória (RMGV) para comparação
    grande_vitoria = {
        normalizar_municipio(n)
        for n in [
            "Vitória",
            "Vila Velha",
            "Serra",
            "Cariacica",
            "Viana",
            "Guarapari",
            "Fundão",
        ]
    }

    if 'Municipio' in df.columns and not df.empty:
        df_tmp = df.copy()

        # Otimização: Garantir que 'Obito' seja tratado como numérico para as agregações (sum/mean)
        if 'Obito' in df_tmp.columns:
            df_tmp['Obito'] = df_tmp['Obito'].astype(int)

        df_tmp['Municipio_norm'] = df_tmp['Municipio'].map(normalizar_municipio)
        df_tmp['MacroRegiao'] = np.where(df_tmp['Municipio_norm'].isin(grande_vitoria), 'Grande Vitória', 'Interior')

        # KPIs de concentração
        dist = df_tmp['MacroRegiao'].value_counts(dropna=False)
        casos_gv = int(dist.get('Grande Vitória', 0))
        casos_int = int(dist.get('Interior', 0))
        total = int(len(df_tmp))
        pct_gv = (casos_gv / total * 100) if total else 0

        c1, c2, c3 = st.columns(3)
        c1.metric("Casos (Grande Vitória)", f"{casos_gv:,}".replace(',', '.'))
        c2.metric("Casos (Interior)", f"{casos_int:,}".replace(',', '.'))
        c3.metric("% na Grande Vitória", f"{pct_gv:.2f}%")

        # Agregar por município para mapear e ranking
        df_stats_mun = (
            df_tmp.groupby('Municipio_norm', dropna=False)
            .agg(
                Casos=('Municipio_norm', 'size'),
                Obitos=('Obito', 'sum')
            )
            .reset_index()
        )
        # Calcular Taxa de Letalidade para o ranking de risco
        df_stats_mun['Taxa Letalidade (%)'] = (df_stats_mun['Obitos'] / df_stats_mun['Casos']) * 100
        
        # Filtro para evitar distorções em municípios com pouquíssimos casos (ex: < 50 casos)
        df_top_risco = df_stats_mun[df_stats_mun['Casos'] >= 50].sort_values('Taxa Letalidade (%)', ascending=False).head(10)

        # Mapa em largura total
        try:
            geojson = carregar_geojson_municipios_es()

            # Padronizar o nome do município no GeoJSON (pode ser "name" ou "nome")
            for f in geojson.get('features', []):
                props = f.get('properties', {})
                props['nome_norm'] = normalizar_municipio(_get_geojson_municipio_nome(props))

            # Diagnóstico rápido: quantos municípios casaram (evita "mapa branco")
            geo_muns = {
                feat.get('properties', {}).get('nome_norm', '')
                for feat in geojson.get('features', [])
            }
            df_muns = set(df_stats_mun['Municipio_norm'].dropna().astype(str))
            matched = len(df_muns & geo_muns)
            
            if matched > 0:
                st.caption(f"Municípios casados no mapa: {matched}/{len(geo_muns)}")

            fig = px.choropleth(
                df_stats_mun,
                geojson=geojson,
                locations='Municipio_norm',
                featureidkey='properties.nome_norm',
                color='Casos',
                color_continuous_scale='Reds',
                hover_name='Municipio_norm',
                labels={'Casos': 'Casos Conf.'},
                hover_data={'Casos': ':,', 'Obitos': ':,', 'Taxa Letalidade (%)': ':.2f'}
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=600)
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.info("Não foi possível carregar o mapa agora.")
            st.caption(f"Detalhe: {e}")

        # Gráfico de barras embaixo do mapa
        st.markdown("---")
        st.markdown("##### ⚠️ Top 10 Municípios com maior Risco (Letalidade)")
        if not df_top_risco.empty:
            fig_bar = px.bar(
                df_top_risco,
                x='Taxa Letalidade (%)',
                y='Municipio_norm',
                orientation='h',
                color='Taxa Letalidade (%)',
                color_continuous_scale='Reds',
                text_auto='.2f',
                labels={'Municipio_norm': 'Município', 'Taxa Letalidade (%)': 'Taxa de Letalidade (%)'}
            )
            fig_bar.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                margin={"r": 20, "t": 10, "l": 10, "b": 10},
                height=450,
                showlegend=False,
                coloraxis_showscale=True
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Dados insuficientes para o ranking.")

    # --------------------
    # Comorbidades e Perfil Etário
    # --------------------
    st.divider()
    st.subheader("Análise de Risco e Comorbidades")
    
    col_c1, col_c2 = st.columns(2)

    with col_c1:
        # Gráfico de Comorbidades (Prevalência vs Letalidade)
        comorb_cols = {
            'ComorbidadeDiabetes': 'Diabetes',
            'ComorbidadeCardio': 'Cardiopatia',
            'ComorbidadePulmao': 'Pulmonar',
            'ComorbidadeRenal': 'Renal',
            'ComorbidadeTabagismo': 'Tabagismo',
            'ComorbidadeObesidade': 'Obesidade'
        }
        
        existentes = [c for c in comorb_cols.keys() if c in df.columns]
        
        if existentes:
            # Calculando Prevalência (Volume) e Letalidade (Risco) para cada comorbidade
            resumo_comorb = []
            for col, nome in comorb_cols.items():
                if col in df.columns:
                    # Pacientes que possuem esta comorbidade
                    df_sub = df[df[col] == True]
                    total_casos = len(df_sub)
                    obitos = df_sub['Obito'].sum() if total_casos > 0 else 0
                    letalidade = (obitos / total_casos * 100) if total_casos > 0 else 0
                    resumo_comorb.append({
                        'Comorbidade': nome,
                        'Volume (Casos)': total_casos,
                        'Risco de Óbito (%)': letalidade
                    })
            
            df_comorb_final = pd.DataFrame(resumo_comorb).sort_values('Volume (Casos)', ascending=True)

            # Gráfico 1: Prevalência (Nº de Pacientes)
            fig_comorb = px.bar(
                df_comorb_final,
                x='Volume (Casos)',
                y='Comorbidade',
                orientation='h',
                title="Volume de Casos por Comorbidade",
                labels={'Volume (Casos)': 'Nº de Pacientes', 'Comorbidade': ''},
                template="plotly_white",
                color_discrete_sequence=['#455A64'] # Azul acinzentado sóbrio
            )
            fig_comorb.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
            st.plotly_chart(fig_comorb, use_container_width=True)

            # Gráfico 2: Letalidade (Risco)
            df_comorb_risco = df_comorb_final.sort_values('Risco de Óbito (%)', ascending=True)
            fig_risco = px.bar(
                df_comorb_risco,
                x='Risco de Óbito (%)',
                y='Comorbidade',
                orientation='h',
                title="Qual comorbidade mais mata? (Letalidade %)",
                labels={'Risco de Óbito (%)': 'Chance de Óbito (%)', 'Comorbidade': ''},
                template="plotly_white",
                color_discrete_sequence=['#C62828'] # Vermelho sóbrio (Risco)
            )
            fig_risco.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
            fig_risco.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350, xaxis_ticksuffix="%")
            st.plotly_chart(fig_risco, use_container_width=True)
        else:
            st.info("Colunas de comorbidades não encontradas.")

    with col_c2:
        # Pirâmide Etária (Simplificada em barras por faixa)
        if 'IdadeNaDataNotificacao' in df.columns:
            # Função para extrair apenas o número do ano da string "X anos, Y meses..."
            def sugerir_idade_limpa(val):
                if pd.isna(val): return np.nan
                s = str(val).lower()
                # Se contiver 'ano', pegamos o que vem antes de 'ano'
                if 'ano' in s:
                    try:
                        return int(s.split('ano')[0].strip().split()[-1])
                    except:
                        return 0
                # Se for só número (formato antigo/limpo)
                try:
                    return int(float(s.split()[0]))
                except:
                    return 0

            # Limpar a coluna de idade para lidar com o formato '0 anos, 6 meses...'
            # Usando uma cópia para evitar warnings e garantir cálculo de risco
            df_age = df[['IdadeNaDataNotificacao', 'Obito']].copy()
            df_age['idade_num'] = df_age['IdadeNaDataNotificacao'].apply(sugerir_idade_limpa)
            
            # Criar faixas etárias
            bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 150]
            labels = ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '80+']
            
            df_age['Faixa Etária'] = pd.cut(df_age['idade_num'], bins=bins, labels=labels, right=False)
            
            # Agrupar por faixa para Casos e Risco
            df_perfil_age = df_age.groupby('Faixa Etária').agg(
                Casos=('idade_num', 'size'),
                Obitos=('Obito', 'sum')
            ).reset_index()
            df_perfil_age['Risco de Óbito (%)'] = (df_perfil_age['Obitos'] / df_perfil_age['Casos']) * 100

            # Gráfico 1: Distribuição de Casos
            fig_idade = px.bar(
                df_perfil_age,
                x='Faixa Etária',
                y='Casos',
                title="Distribuição Etária (Volume)",
                labels={'Casos': 'Casos Conf.', 'Faixa Etária': 'Anos'},
                template="plotly_white",
                color_discrete_sequence=['#78909C'] # Cinza azulado claro
            )
            fig_idade.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
            st.plotly_chart(fig_idade, use_container_width=True)

            # Gráfico 2: Risco por Idade
            fig_risco_age = px.bar(
                df_perfil_age,
                x='Faixa Etária',
                y='Risco de Óbito (%)',
                title="Risco de Óbito por Faixa Etária (%)",
                labels={'Risco de Óbito (%)': 'Letalidade (%)', 'Faixa Etária': 'Anos'},
                template="plotly_white",
                color_discrete_sequence=['#546E7A'] # Cinza azulado escuro
            )
            fig_risco_age.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
            fig_risco_age.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350, yaxis_ticksuffix="%")
            st.plotly_chart(fig_risco_age, use_container_width=True)
        else:
            st.info("Coluna de idade não encontrada.")

    # --------------------
    # Análise de Sobrevivência (Processo KDD Avançado)
    # --------------------
    st.divider()
    st.subheader("Processo de Limpeza e Análise de Sobrevivência (KDD)")
    
    # 1. Criação do Atributo Derivado (Feature Engineering)
    df_sobrevida = df[df['Obito'] == True].copy()
    df_sobrevida['DataNotificacao'] = pd.to_datetime(df_sobrevida['DataNotificacao'], errors='coerce')
    df_sobrevida['DataObito'] = pd.to_datetime(df_sobrevida['DataObito'], errors='coerce')
    df_sobrevida = df_sobrevida.dropna(subset=['DataNotificacao', 'DataObito'])
    
    # Atributo Derivado: Dias de Sobrevida
    df_sobrevida['Dias_Sobrevida'] = (df_sobrevida['DataObito'] - df_sobrevida['DataNotificacao']).dt.days
    
    # Filtrar apenas dias positivos (ignorar notificações pós-morte para esta análise)
    df_base = df_sobrevida[df_sobrevida['Dias_Sobrevida'] >= 0].copy()

    if not df_base.empty:
        # 2. Detecção de Outliers via IQR (Padrão-Ouro de BI)
        Q1 = df_base['Dias_Sobrevida'].quantile(0.25)
        Q3 = df_base['Dias_Sobrevida'].quantile(0.75)
        IQR = Q3 - Q1
        
        limite_inferior = Q1 - 1.5 * IQR
        limite_superior = Q3 + 1.5 * IQR
        
        df_limpo = df_base[(df_base['Dias_Sobrevida'] >= limite_inferior) & 
                           (df_base['Dias_Sobrevida'] <= limite_superior)].copy()
        
        st.write("### 🧪 KDD: Tratamento de Outliers (IQR)")
        st.info(f"O Intervalo Interquartil (IQR) definiu registros acima de {int(limite_superior)} dias como outliers estatísticos.")
        
        col_box1, col_box2 = st.columns(2)
        
        with col_box1:
            fig_box_pre = px.box(
                df_base, 
                y='Dias_Sobrevida', 
                title="Antes: Com Outliers",
                labels={'Dias_Sobrevida': 'Dias'},
                template="plotly_white",
                color_discrete_sequence=['#EF5350']
            )
            fig_box_pre.update_layout(height=400)
            st.plotly_chart(fig_box_pre, use_container_width=True)
            
        with col_box2:
            fig_box_pos = px.box(
                df_limpo, 
                y='Dias_Sobrevida', 
                title="Depois: Limpeza IQR",
                labels={'Dias_Sobrevida': 'Dias'},
                template="plotly_white",
                color_discrete_sequence=['#66BB6A']
            )
            fig_box_pos.update_layout(height=400)
            st.plotly_chart(fig_box_pos, use_container_width=True)

        # 3. Estatísticas Descritivas do Dado "Saneado"
        media_saneada = df_limpo['Dias_Sobrevida'].mean()
        mediana_saneada = df_limpo['Dias_Sobrevida'].median()
        
        modal_calc = df_limpo[df_limpo['Dias_Sobrevida'] > 0]['Dias_Sobrevida'].mode()
        moda_saneada = modal_calc[0] if not modal_calc.empty else 0
        
        st.markdown(f"#### Estatísticas Saneadas (Pós-Limpeza)")
        cs1, cs2, cs3 = st.columns(3)
        cs1.metric("Média Real", f"{media_saneada:.1f} dias", delta=f"{media_saneada - df_base['Dias_Sobrevida'].mean():.1f} vs brute", delta_color="inverse")
        cs2.metric("Mediana", f"{int(mediana_saneada)} dias")

        # Histograma do Dado Limpo
        fig_hist_final = px.histogram(
            df_limpo,
            x='Dias_Sobrevida',
            nbins=30,
            title="Distribuição Final da Sobrevida (Dados Saneados)",
            labels={'Dias_Sobrevida': 'Dias após Notificação', 'count': 'Frequência'},
            template="plotly_white",
            color_discrete_sequence=['#455A64']
        )
        fig_hist_final.add_vline(x=media_saneada, line_dash="dash", line_color="red", annotation_text=f"Média: {media_saneada:.1f}")
        fig_hist_final.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=400)
        st.plotly_chart(fig_hist_final, use_container_width=True)
        
        st.caption(f"💡 *O processo removeu {len(df_base) - len(df_limpo)} registros considerados ruído estatístico, resultando em uma média muito mais próxima da realidade clínica.*")
    else:
        st.info("Dados de datas insuficientes para o processo KDD.")

    # --------------------
    # Análise Temporal de Letalidade
    # --------------------
    st.divider()
    st.subheader("Evolução Temporal da Taxa de Letalidade (%)")
    
    # Criar DataFrame para análise temporal
    df_temporal = df[['DataNotificacao', 'Obito']].copy()
    df_temporal['DataNotificacao'] = pd.to_datetime(df_temporal['DataNotificacao'], errors='coerce')
    df_temporal = df_temporal.dropna(subset=['DataNotificacao'])
    
    # Agrupar por Mês/Ano para ver a evolução da taxa
    df_temporal['Mes_Ano'] = df_temporal['DataNotificacao'].dt.to_period('M').astype(str)
    
    # Calcular taxa por período
    stats_tempo = df_temporal.groupby('Mes_Ano').agg(
        Casos=('Obito', 'size'),
        Obitos=('Obito', 'sum')
    ).reset_index()
    
    stats_tempo['Taxa Letalidade (%)'] = (stats_tempo['Obitos'] / stats_tempo['Casos']) * 100
    
    # Identificar o pico de letalidade
    pico = stats_tempo.loc[stats_tempo['Taxa Letalidade (%)'].idxmax()]
    
    st.markdown(f"##### 📈 Resultado da Análise Temporal")
    ct1, ct2 = st.columns([1, 3])
    
    with ct1:
        st.write("")
        st.metric("Pior Período (Letalidade)", pico['Mes_Ano'])
        st.metric("Taxa no Pico", f"{pico['Taxa Letalidade (%)']:.2f}%")
        st.caption(f"No mês de {pico['Mes_Ano']}, a letalidade foi a mais alta da série histórica.")

    with ct2:
        fig_evolucao = px.line(
            stats_tempo,
            x='Mes_Ano',
            y='Taxa Letalidade (%)',
            title="Taxa de Letalidade Mensal ao Longo do Tempo",
            labels={'Mes_Ano': 'Mês de Notificação', 'Taxa Letalidade (%)': 'Letalidade (%)'},
            template="plotly_white",
            markers=True
        )
        # Destacar o pico com uma anotação ou cor
        fig_evolucao.add_annotation(
            x=pico['Mes_Ano'], 
            y=pico['Taxa Letalidade (%)'],
            text="PICO",
            showarrow=True,
            arrowhead=1,
            arrowcolor="#C62828",
            font=dict(color="#C62828", size=12)
        )
        fig_evolucao.update_traces(line_color='#455A64')
        fig_evolucao.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=400)
        st.plotly_chart(fig_evolucao, use_container_width=True)

    # --------------------
    # Análise de Recuperados (Cura)
    # --------------------
    st.divider()
    st.subheader("Análise de Pacientes Recuperados (Cura)")

    # Filtrar pacientes com Status de Cura
    df_cura = df[df['Status_Analise'] == 'Recuperado'].copy()
    
    if not df_cura.empty:
        col_cura1, col_cura2 = st.columns(2)
        
        with col_cura1:
            # Perfil de Comorbidades em quem se recuperou
            comorb_cols_cura = {
                'ComorbidadeDiabetes': 'Diabetes',
                'ComorbidadeCardio': 'Cardiopatia',
                'ComorbidadePulmao': 'Pulmonar',
                'ComorbidadeRenal': 'Renal',
                'ComorbidadeTabagismo': 'Tabagismo',
                'ComorbidadeObesidade': 'Obesidade'
            }
            existentes_cura = [c for c in comorb_cols_cura.keys() if c in df_cura.columns]
            
            if existentes_cura:
                dados_comorb_cura = df_cura[existentes_cura].sum().reset_index()
                dados_comorb_cura.columns = ['Comorbidade', 'Total']
                dados_comorb_cura['Comorbidade'] = dados_comorb_cura['Comorbidade'].map(comorb_cols_cura)
                dados_comorb_cura = dados_comorb_cura.sort_values('Total', ascending=True)

                fig_comorb_cura = px.bar(
                    dados_comorb_cura,
                    x='Total',
                    y='Comorbidade',
                    orientation='h',
                    title="Comorbidades em Pacientes Recuperados",
                    labels={'Total': 'Nº de Recuperados', 'Comorbidade': ''},
                    template="plotly_white",
                    color_discrete_sequence=['#66BB6A'] # Verde sóbrio
                )
                fig_comorb_cura.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                st.plotly_chart(fig_comorb_cura, use_container_width=True)

        with col_cura2:
            # Distribuição Etária de quem se recuperou
            if 'IdadeNaDataNotificacao' in df_cura.columns:
                df_cura['idade_num'] = df_cura['IdadeNaDataNotificacao'].apply(sugerir_idade_limpa)
                bins_cura = [0, 10, 20, 30, 40, 50, 60, 70, 80, 150]
                labels_cura = ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '80+']
                df_cura['Faixa Etária'] = pd.cut(df_cura['idade_num'], bins=bins_cura, labels=labels_cura, right=False)
                
                counts_cura = df_cura['Faixa Etária'].value_counts().reset_index()
                counts_cura.columns = ['Faixa Etária', 'Recuperados']
                counts_cura = counts_cura.sort_values('Faixa Etária')

                fig_idade_cura = px.bar(
                    counts_cura,
                    x='Faixa Etária',
                    y='Recuperados',
                    title="Distribuição Etária dos Recuperados",
                    labels={'Recuperados': 'Nº de Pessoas', 'Faixa Etária': 'Anos'},
                    template="plotly_white",
                    color_discrete_sequence=['#81C784'] # Verde claro
                )
                fig_idade_cura.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                st.plotly_chart(fig_idade_cura, use_container_width=True)

        # Métrica de Taxa de Recuperação Global
        total_casos_val = len(df[df['Status_Analise'].isin(['Óbito', 'Recuperado'])])
        if total_casos_val > 0:
            taxa_recuperacao = (len(df_cura) / total_casos_val) * 100
            st.info(f"💡 **Taxa de Recuperação Global:** {taxa_recuperacao:.2f}% dos casos com desfecho final confirmado resultaram em cura.")

        # Mapa Coroplético de Taxa de Cura por Município
        st.markdown("##### 🗺️ Mapa Coroplético: Eficiência de Recuperação por Município (%)")
        
        # Preparar dados para o mapa de cura
        df_desfecho = df[df['Status_Analise'].isin(['Óbito', 'Recuperado'])].copy()
        df_desfecho['Municipio_norm'] = df_desfecho['Municipio'].map(normalizar_municipio)
        
        # Agrupar por município normalizado para bater com o GeoJSON
        stats_mapa_cura = df_desfecho.groupby('Municipio_norm').agg(
            Total_Desfechos=('Status_Analise', 'size'),
            Total_Curas=('Status_Analise', lambda x: (x == 'Recuperado').sum())
        ).reset_index()
        
        stats_mapa_cura['Taxa de Cura (%)'] = (stats_mapa_cura['Total_Curas'] / stats_mapa_cura['Total_Desfechos']) * 100
        
        # Filtro de relevância estatística para o mapa (ex: min 50 casos com desfecho para colorir)
        stats_mapa_cura = stats_mapa_cura[stats_mapa_cura['Total_Desfechos'] >= 50]

        try:
            geojson = carregar_geojson_municipios_es()
            # O normalizar_municipio já está sendo aplicado nas propriedades do GeoJSON na função main()
            # mas vamos garantir a consistência aqui se necessário ou usar o objeto já carregado.
            for f in geojson.get('features', []):
                props = f.get('properties', {})
                props['nome_norm'] = normalizar_municipio(_get_geojson_municipio_nome(props))

            fig_mapa_cura = px.choropleth(
                stats_mapa_cura,
                geojson=geojson,
                locations='Municipio_norm',
                featureidkey='properties.nome_norm',
                color='Taxa de Cura (%)',
                # Escala personalizada para dar contraste rápido e destacar diferenças sutis no topo (90-100%)
                color_continuous_scale=[
                    [0, "#E8F5E9"],     # Verde quase branco para o início (baixa cura)
                    [0.5, "#81C784"],   # Verde médio no meio
                    [0.8, "#388E3C"],   # Verde escuro aos 80% da escala
                    [1, "#1B5E20"]      # Verde floresta profundo no máximo (100%)
                ],
                hover_name='Municipio_norm',
                labels={'Taxa de Cura (%)': 'Taxa de Cura (%)'},
                hover_data={'Total_Desfechos': ':,', 'Total_Curas': ':,', 'Taxa de Cura (%)': ':.2f'}
            )
            fig_mapa_cura.update_geos(fitbounds="locations", visible=False)
            fig_mapa_cura.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0}, height=500)
            st.plotly_chart(fig_mapa_cura, use_container_width=True)
            st.caption("ℹ️ *Municípios em branco possuem volume estatístico insuficiente (< 50 casos confirmados com desfecho).*")
        except Exception as e:
            st.info("Não foi possível carregar o mapa de cura agora.")
            st.caption(f"Detalhe: {e}")
    else:
        st.info("Não há dados de pacientes recuperados para exibir.")

if __name__ == "__main__":
    main()
