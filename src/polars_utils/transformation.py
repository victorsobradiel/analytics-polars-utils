import polars as pl

def standard_clean(df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    """
    Remove espaços em branco (trim) de todas as colunas de string 
    e filtra registros deletados padrão Protheus (D_E_L_E_T_).
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    lf = df.lazy() if not is_lazy else df

    cols_to_drop = ["d_e_l_e_t_", "recno_part", "r_e_c_n_o_"]
    existing_cols = lf.collect_schema().names()
    
    lf = (
        lf.rename({col: col.lower() for col in existing_cols})
        .with_columns(pl.col(pl.String).str.strip_chars())
        .drop([c for c in cols_to_drop if c.lower() in [e.lower() for e in existing_cols]])
    )

    return lf if is_lazy else lf.collect()

def parse_protheus_date(col_name: str, alias: str = None) -> pl.Expr:
    """Converte string YYYYMMDD para Date"""
    target = alias if alias else col_name
    return pl.col(col_name).str.strptime(pl.Date, "%Y%m%d", strict=False).alias(target)


def desembaralha_protheus(cstring: str) -> str:
    """
    Versão Python da função Oracle 'DESEMBARALHA' (para INFO=0).
    Realiza o desembaralhamento da string para obter o ID do usuário.
    """
    if cstring is None:
        return None
    
    c_str = cstring.strip()
    if not c_str:
        return ""

    # Lógica 'MODO' do Oracle: length < 17
    modo = len(c_str) < 17
    
    # O loop principal roda 2 vezes (J IN 1 .. 2)
    for j in range(1, 3):
        
        # Lógica do passo J=1
        if j == 1 and not modo:
            # STUFF(CSTR,12,1,' ') e STUFF(CSTR,16,1,' ')
            # Nota: Índices no Oracle são base 1, Python base 0.
            # 12 vira 11, 16 vira 15.
            temp_list = list(c_str)
            if len(temp_list) > 11:
                temp_list[11] = ' '
            if len(temp_list) > 15:
                temp_list[15] = ' '
            c_str = "".join(temp_list)

        # Lógica de Desembaralhamento (NTIMES)
        n_times = len(c_str) // 2
        cembaralha_parts = []
        c_str_list = list(c_str)
        
        # Loop I IN 1 .. NTIMES
        # Oracle: Pega char em (LENGTH - I), remove da string original
        for i in range(1, n_times + 1):
            # No Oracle, LENGTH muda a cada iteração (STUFF remove o char).
            # A posição de remoção é (Len_Atual - i).
            # Ajustando para base 0 do Python: len(lista) - i - 1 (para alinhar com a lógica Oracle de pegar o antepenúltimo relativo)
            idx_to_remove = len(c_str_list) - i - 1 
            
            if idx_to_remove >= 0:
                char_removed = c_str_list.pop(idx_to_remove)
                cembaralha_parts.append(char_removed)
        
        # Loop REVERSE (restante da string)
        cembaralha_parts.extend(reversed(c_str_list))
        
        # Reconstrói a string para a próxima iteração
        c_str = "".join(cembaralha_parts)
    
    # Lógica pós-loop (INFO = 0)
    cembaralha = c_str
    
    if modo:
        # SUBSTR(CEMBARALHA, 5) -> Python [4:]
        cembaralha = cembaralha[4:].strip()
        
    # Verifica padrão '#@' nas posições 5,6 (Python 4,5)
    # Oracle SUBSTR(..., 5, 2)
    if len(cembaralha) >= 6 and cembaralha[4:6] == '#@':
        # Oracle: SUBSTR(..,7,2) || SUBSTR(..,1,4)
        # Python: [6:8] + [0:4]
        part1 = cembaralha[6:8]
        part2 = cembaralha[0:4]
        cembaralha = part1 + part2
        
    return cembaralha.strip()
 
def safe_drop(lf, cols):
    # collect_schema().names() apenas lê os metadados, é instantâneo
    existing_cols = [c for c in cols if c in lf.collect_schema().names()]
    return lf.drop(existing_cols)