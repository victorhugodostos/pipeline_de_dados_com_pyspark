# Pipeline de Processamento de Dados

Este documento apresenta o pipeline de processamento de dados que usamos com Apache Spark e PySpark. Nosso objetivo é mostrar como lemos, processamos e combinamos dados de arquivos JSON e CSV, e depois salvamos o resultado em um arquivo CSV. Vamos explicar a arquitetura, as tecnologias utilizadas e onde você pode encontrar mais informações.

## Como Funciona

O pipeline segue várias etapas para processar os dados:

1. **Iniciar o Spark**:
   - Criamos uma sessão do Spark, que é como um ponto de partida para executar nossas operações de dados.

2. **Ler os Dados**:
   - **Arquivos JSON e CSV**: Usamos funções específicas para ler dados desses arquivos e transformá-los em DataFrames do Spark.
   - **Função `leitura_dados`**: Esta função decide se o arquivo é JSON ou CSV e chama a função correta para ler o arquivo.

3. **Transformar os Dados**:
   - **Renomear Colunas**: Alteramos os nomes das colunas dos DataFrames CSV para garantir que tudo esteja no formato esperado.
   - **Formatar em Tabela**: Ajustamos os dados para garantir que todas as colunas necessárias estejam presentes e na ordem correta.

4. **Combinar os Dados**:
   - **Unir DataFrames**: Juntamos os DataFrames JSON e CSV em um único DataFrame para ter todos os dados em um só lugar.

5. **Salvar os Dados**:
   - **Exportar para CSV**: Salvamos o DataFrame combinado e formatado em um arquivo CSV, que pode ser usado para outras análises ou relatórios.

6. **Encerrar o Spark**:
   - **Finalizar a Sessão**: Fechamos a sessão do Spark para liberar os recursos que estávamos usando.

## Tecnologias Usadas

### Apache Spark

- **Versão**: 3.4.0
- **O que é**: Uma plataforma poderosa para processamento de grandes volumes de dados, distribuída em múltiplos servidores.

### PySpark

- **Versão**: 3.4.0
- **O que é**: A versão do Spark para Python, que permite trabalhar com dados grandes usando a linguagem Python.

### Python

- **Versão**: 3.8 ou superior
- **O que é**: A linguagem de programação usada para escrever o código, ideal para análise e manipulação de dados.

### CSV e JSON

- **CSV**: Formato de texto simples com dados separados por vírgulas, muito usado para armazenar dados tabulares.
- **JSON**: Formato leve e fácil de ler que representa dados estruturados.

## Referências

- **Apache Spark**: [Site Oficial](https://spark.apache.org/)
- **Documentação do PySpark**: [Documentação PySpark](https://spark.apache.org/docs/latest/api/python/)
- **Python**: [Documentação Oficial](https://www.python.org/doc/)

## Dicas

- **Estudo**: O pipeline foi criado para aprendizado e experimentação, para ajudar a entender e aplicar técnicas de processamento de dados com PySpark.
- **Preparação dos Arquivos**: Verifique se os arquivos JSON e CSV estão formatados corretamente e estão nos locais certos para evitar problemas durante a execução.


Se precisar de ajuda ou encontrar algum problema com o pipeline, não hesite em me chamar. Estou por aqui para esclarecer dúvidas ou ajudar a resolver qualquer questão. Conte comigo para garantir que tudo funcione direitinho!
