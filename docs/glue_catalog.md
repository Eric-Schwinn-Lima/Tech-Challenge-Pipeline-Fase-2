# Correção do Glue Data Catalog e dos Crawlers

## 1. Diagnóstico de 03/11/2026

A estrutura física do Data Lake está particionada por execução:

~~~text
s3://<bucket>/silver/<tabela>/run_id=<id>/*.parquet
s3://<bucket>/gold/<tabela>/run_id=<id>/*.parquet
~~~

Os Crawlers têm como alvos as raízes silver/ e gold/. Portanto, contando a partir da raiz do bucket, a camada é o nível 1, o diretório da tabela é o nível 2 e run_id=... é o nível 3.

A inspeção de 03/11/2026 encontrou 40 entradas no Glue Data Catalog: 16 tabelas principais (8 Silver e 8 Gold) e 24 tabelas adicionais com nomes run_id_*. A inspeção de duas amostras mostrou que:
- a tabela principal, por exemplo uf, possui run_id em PartitionKeys e localização no diretório silver/uf/;
- a tabela adicional run_id_... aponta para silver/uf/run_id=.../ e não tem chaves de partição.

Isso confirma que o catálogo acumulou registros de partições como tabelas independentes. Os arquivos e o particionamento no S3 não precisam ser reescritos para corrigir esse problema.

## 2. Correção no Terraform

Os dois recursos aws_glue_crawler.silver e aws_glue_crawler.gold em cloud/terraform/main.tf agora declaram:

~~~hcl
configuration = jsonencode({
  Version = 1.0
  Grouping = {
    TableLevelConfiguration = 2
  }
})
~~~

O nível 2 é deliberado:
- nível 1: silver/ ou gold/;
- nível 2: nome da tabela;
- nível 3: diretório run_id=..., que deve ser uma partição.

A política DeleteBehavior = LOG é mantida. Não altere para exclusão automática: mudanças no catálogo devem ser revisadas e a limpeza dos registros legados será seletiva.

## 3. Aplicar a mudança sem reexecutar o Glue Job

A mudança altera somente a configuração dos dois Crawlers. Ela não precisa reprocessar os CSVs nem regravar Silver/Gold.

No PowerShell, no clone atualizado:

~~~powershell
git pull origin main

terraform -chdir=cloud/terraform fmt -check -recursive
terraform -chdir=cloud/terraform validate
terraform -chdir=cloud/terraform plan -out=glue-crawler-level-fix.tfplan
terraform -chdir=cloud/terraform show glue-crawler-level-fix.tfplan
~~~

**Antes do apply:** confira o plano completo. Para esta correção, o esperado é atualização in-place de aws_glue_crawler.silver e aws_glue_crawler.gold, adicionando o campo configuration. Não prossiga se houver destruição, substituição do bucket, exclusão de objetos S3, alteração do Glue Job ou mudanças não relacionadas.

Se o plano corresponder ao esperado:

~~~powershell
terraform -chdir=cloud/terraform apply glue-crawler-level-fix.tfplan
~~~

Confirme o valor aplicado na AWS:

~~~powershell
$silverCrawler = "tc-alfabetizacao-a9455abd-silver-crawler"
$goldCrawler = "tc-alfabetizacao-a9455abd-gold-crawler"

aws glue get-crawler --name $silverCrawler --profile AWS-tech-challenge --region us-east-2 --query "Crawler.Configuration" --output text
aws glue get-crawler --name $goldCrawler --profile AWS-tech-challenge --region us-east-2 --query "Crawler.Configuration" --output text
~~~

O JSON deve conter "TableLevelConfiguration":2.

## 4. Recatalogar os arquivos já existentes no S3

Não é necessário disparar a Step Functions apenas para corrigir o catálogo: a máquina também executaria novamente o Glue Job, gerando outra execução e custo. Após aplicar a configuração dos Crawlers, faça a recatalogação diretamente e em série:

~~~powershell
aws glue start-crawler --name $silverCrawler --profile AWS-tech-challenge --region us-east-2

do {
    Start-Sleep -Seconds 10
    $silverState = aws glue get-crawler --name $silverCrawler --profile AWS-tech-challenge --region us-east-2 --query "Crawler.State" --output text
    Write-Host "Silver: $silverState"
} while ($silverState -ne "READY")

aws glue get-crawler --name $silverCrawler --profile AWS-tech-challenge --region us-east-2 --query "Crawler.LastCrawl.Status" --output text

aws glue start-crawler --name $goldCrawler --profile AWS-tech-challenge --region us-east-2

do {
    Start-Sleep -Seconds 10
    $goldState = aws glue get-crawler --name $goldCrawler --profile AWS-tech-challenge --region us-east-2 --query "Crawler.State" --output text
    Write-Host "Gold: $goldState"
} while ($goldState -ne "READY")

aws glue get-crawler --name $goldCrawler --profile AWS-tech-challenge --region us-east-2 --query "Crawler.LastCrawl.Status" --output text
~~~

Os dois últimos comandos devem retornar SUCCEEDED. Confirme também que tabelas principais como uf e fato_alfabetizacao_municipio mantêm run_id em PartitionKeys e apontam para o diretório raiz de suas tabelas.

## 5. Identificar metadados legados para remoção

Como DeleteBehavior = LOG, o Crawler não remove automaticamente as 24 tabelas antigas. Depois de confirmar que os Crawlers corrigidos foram executados com sucesso, o bloco abaixo identifica apenas tabelas que atendem a todas estas condições:
1. nome começa com run_id_;
2. não tem chaves de partição;
3. localização é exatamente um diretório de partição abaixo de uma tabela Silver ou Gold do bucket deste projeto.

O primeiro bloco apenas lista candidatos. Revise os nomes e os locais antes de autorizar a exclusão de metadados.

~~~powershell
$database = "tc_alfabetizacao_a9455abd_catalog"
$bucket = "tc-alfabetizacao-a9455abd-lake"
$profile = "AWS-tech-challenge"
$region = "us-east-2"
$catalog = aws glue get-tables --database-name $database --profile $profile --region $region --output json | ConvertFrom-Json

$safeLocation = "^s3://$([regex]::Escape($bucket))/(silver|gold)/[^/]+/run_id=[^/]+/$"

$candidates = @(
    $catalog.TableList | Where-Object {
        $_.Name -like "run_id_*" -and
        @($_.PartitionKeys).Count -eq 0 -and
        $_.StorageDescriptor.Location -match $safeLocation
    }
)

Write-Host "Candidatos encontrados: $($candidates.Count)"
$candidates |
    Select-Object Name, @{Name="Location";Expression={$_.StorageDescriptor.Location}} |
    Format-Table -AutoSize
~~~

Se a lista contiver somente os registros redundantes esperados, a exclusão do Glue Data Catalog pode ser autorizada digitando exatamente a frase abaixo. Isso exclui metadados, não arquivos S3:

~~~powershell
$confirmation = Read-Host "Digite APAGAR SOMENTE METADADOS CATALOGADOS para continuar"

if ($confirmation -ceq "APAGAR SOMENTE METADADOS CATALOGADOS") {
    foreach ($table in $candidates) {
        Write-Host "Removendo somente metadados: $($table.Name)"
        aws glue delete-table --database-name $database --name $table.Name --profile $profile --region $region

        if ($LASTEXITCODE -ne 0) {
            throw "Falha ao remover $($table.Name). Interrompendo a limpeza."
        }
    }
} else {
    Write-Host "Operação cancelada. Nenhum metadado foi removido."
}
~~~

Não use aws s3 rm, não altere os arquivos Parquet e não remova diretórios run_id do bucket. As pastas são dados históricos válidos.

## 6. Validação final

Reconte as tabelas:

~~~powershell
$rawNames = aws glue get-tables --database-name $database --profile $profile --region $region --query "TableList[].Name" --output text
$names = @($rawNames -split '\s+' | Where-Object { $_ -and $_.Trim() })
$main = @($names | Where-Object { $_ -notlike "run_id_*" })
$legacy = @($names | Where-Object { $_ -like "run_id_*" })

Write-Host "Total: $($names.Count)"
Write-Host "Tabelas principais: $($main.Count)"
Write-Host "Tabelas run_id remanescentes: $($legacy.Count)"
$main | Sort-Object
$legacy | Sort-Object
~~~

Após a limpeza dos 24 registros identificados, o catálogo deve exibir as 16 tabelas principais esperadas (8 Silver e 8 Gold) e nenhuma tabela run_id_*. O número final precisa ser confirmado na conta; não registrar como concluído antes dessa verificação.

Valide amostras do catálogo:

~~~powershell
aws glue get-table --database-name $database --name uf --profile $profile --region $region --query "Table.{Location:StorageDescriptor.Location,PartitionKeys:PartitionKeys[].Name}" --output json

aws glue get-table --database-name $database --name fato_alfabetizacao_municipio --profile $profile --region $region --query "Table.{Location:StorageDescriptor.Location,PartitionKeys:PartitionKeys[].Name}" --output json
~~~

O esperado em ambos é localização na raiz da tabela e PartitionKeys = ["run_id"]. Confirme também que Athena consulta a execução desejada com filtro WHERE run_id = '<id>'.

## 7. Segurança e reversão

- O terraform plan deve ser revisado antes de qualquer aplicação.
- A correção não altera o S3 e não exige reprocessar a camada Bronze.
- O script de limpeza remove apenas entradas do Data Catalog que passam pelos três filtros de segurança e exige confirmação digitada.
- Se uma tabela candidata não corresponder ao padrão descrito, não a exclua; investigue a localização e o consumidor antes.
- Não altere DeleteBehavior = LOG para DELETE_FROM_DATABASE como atalho.
- A limpeza de metadados não é reversível pelo Glue. Os arquivos S3 permanecem intactos e permitem recatalogação posterior com o crawler corrigido.

## Referência AWS

- [AWS Glue — especificar a localização da tabela e o nível de particionamento](https://docs.aws.amazon.com/pt_br/glue/latest/dg/crawler-table-level.html)
- [AWS Glue — como os Crawlers determinam tabelas e partições](https://docs.aws.amazon.com/pt_br/glue/latest/dg/add-crawler.html)
