import pandas as pd
import streamlit as st
import numpy as np

@st.cache_data
def carregar_dados_es():
    """
    Carrega a base consolidada do ES no formato otimizado Parquet.
    Formatos colunares são lidos em milissegundos e preservam tipos nativos 
    como Categorias e Dates automaticamente!
    """
    df = pd.read_parquet('dados_es_filtrados.parquet')
    
    # Remover registros com inconsistência: Evolucao indica Óbito, mas DataObito é nula
    obitos_mask = df['Evolucao'].astype(str).str.contains('bito', na=False, case=False)
    inconsistentes = obitos_mask & df['DataObito'].isnull()
    df = df[~inconsistentes]
    
    # --- 🌟 MÉTRICA DE OURO: STATUS HONESTO (MUNDO REAL) ---
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

def main():
    st.set_page_config(page_title="Painel COVID-19 ES", layout="wide")
    st.title("Painel COVID-19 - Espírito Santo")

    # Mostrar o processo KDD num Expander na barra lateral
    with st.sidebar.expander("🛠️ Processo KDD e Qualidade de Dados", expanded=False):
        st.markdown('''
        **Knowledge Discovery in Databases (KDD)**
        
        Durante a preparação e carga destes dados, aplicamos tratamentos rigorosos de Engenharia de Dados para garantir a qualidade analítica:
        
        1. **Big Data (Parquet)**: O arquivo que originalmente pesava 876MB em CSV foi comprimido e convertido para um formato colunar Parquet (44MB). Isso reduziu o I/O, preservou tipagens em C e entrega tempos de carregamento na casa dos milissegundos.
        2. **Otimização de Memória**: Atributos de texto massivamente repetitivos (*Municípios*, *Comorbidades*, *Evolução*) foram fixados como `category`, poupando GBs de memória RAM do servidor.
        3. **Limpeza de Inconsistências**: Filtrados e removidos 166 registros "fantasmas" onde a evolução indicava óbito, mas o campo de Data do Óbito estava vazio.
        4. **Métrica "Status Honesto"**: Cientes da complexidade (e sujeira) do mundo real, vetomizamos em milissegundos uma classificação em 4 níveis cruzando as colunas `Evolucao` e `DataObito`. Agora podemos agrupar os pacientes diretamente nas cestas absolutas: **Óbito, Recuperado, Em Aberto / Ignorado, e Outros**.
        ''')

    with st.spinner("Carregando e processando a base filtrada do ES..."):
        df = carregar_dados_es()
    
    st.success(f"Base de dados carregada! Total de linhas: {df.shape[0]:,} | Colunas: {df.shape[1]}".replace(',', '.'))
    
    # Divisão de KPIs em colunas
    st.subheader("Indicadores Gerais de Saúde")
    
    # Calcular métricas principais usando a Métrica Honesta nativa
    total_casos = len(df)
    
    # Identificar óbitos (Pelo Status Honesto)
    df['Obito_Binario'] = (df['Status_Analise'] == 'Óbito').astype(int)
    total_obitos = df['Obito_Binario'].sum()
    taxa_letalidade = (total_obitos / total_casos) * 100 if total_casos > 0 else 0
    
    # Criar 3 colunas para os KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Casos Confirmados", f"{total_casos:,}".replace(',', '.'))
    col2.metric("Total de Óbitos", f"{total_obitos:,}".replace(',', '.'))
    col3.metric("Taxa de Letalidade (T.L.)", f"{taxa_letalidade:.2f}%")
    
    st.divider()

    st.subheader("Desempenho por Município (T.L.)")
    st.markdown("A tabela abaixo agrupa dados por município para facilitar a tomada de decisão focada na taxa de letalidade.")
    
    # Agrupar dados por município (ignorando ausentes)
    df_municipio = df.groupby('Municipio')['Obito_Binario'].agg(
        Casos='count',
        Obitos='sum'
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
    
    with st.expander("Visualizar Microdados (Amostra de 100 linhas)"):
        st.dataframe(df.drop(columns=['Obito_Binario']).head(100), use_container_width=True)

if __name__ == "__main__":
    main()
