> 💡 **Acesso Programático:**  
> Todas as tabelas de suporte descritas nesta página podem ser carregadas dinamicamente em memória através do método `get_suportes()` da classe `Motor` (localizada em `motor_de_pricing/motor.py`)[cite: 1].  
> Além disso, você pode utilizar o método `.listar_suportes()` no objeto retornado para consultar todas as tabelas disponíveis.

### 💻 Exemplo de Uso em Python

```python
from motor_de_pricing.motor import Motor

# 1. Instancia o motor principal
motor = Motor()

# 2. Obtém o objeto com as tabelas de suporte recarregadas em memória
suportes = motor.get_suportes()

# 3. Lista os nomes de todas as tabelas de suporte disponíveis
tabelas_disponiveis = suportes.listar_suportes()
print("Tabelas disponíveis:", tabelas_disponiveis)

# 4. Acessa uma tabela específica (ex: regras de intercâmbio)
df_regras_intercambio = suportes.get("regras_intercambio")
print(df_regras_intercambio)
