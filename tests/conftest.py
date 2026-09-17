from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def valid_month_df() -> pd.DataFrame:
    """
    Small valid FIPE monthly snapshot used across tests.

    Grain:
    ano_referencia + mes_referencia + codigo_fipe
    + ano_modelo + sigla_combustivel
    """
    return pd.DataFrame(
        {
            "tipo_veiculo": ["carro", "carro"],
            "codigo_fipe": ["001001-1", "001002-0"],
            "nome_modelo": ["Modelo A", "Modelo B"],
            "nome_marca": ["Marca A", "Marca B"],
            "nome_combustivel": ["Gasolina", "Flex"],
            "sigla_combustivel": ["g", "f"],
            "ano_modelo": pd.Series([2025, 2026], dtype="Int64"),
            "zero_km": [False, False],
            "valor_centavos": [100_000_00, 150_000_00],
            "valor_formatado": ["R$ 100.000,00", "R$ 150.000,00"],
            "mes_referencia": pd.Series([9, 9], dtype="int32"),
            "ano_referencia": pd.Series([2026, 2026], dtype="int32"),
        }
    )
