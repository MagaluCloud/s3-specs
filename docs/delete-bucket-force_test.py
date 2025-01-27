import pytest
import logging
from s3_helpers import run_example
import botocore.exceptions

# Configuração do logger para mostrar as informações de execução do teste
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Caminho do arquivo de configuração YAML (pode ser alterado conforme o ambiente de teste)
config = "../params/br-se1.yaml"

# Função auxiliar para adicionar o cabeçalho 'x-force-container-delete' nas requisições, dependendo do valor do parâmetro 'force'
def _add_header(request, force):
    request.headers['x-force-container-delete'] = force
    logger.info(f"Headers atualizados: {request.headers}")

# Parametrização do teste, incluindo múltiplos clientes S3, e o parâmetro 'force' para controle do comportamento de exclusão de bucket
@pytest.mark.parametrize(
    'multiple_s3_clients, force, bucket_with_one_object', [
        ({"number_clients": 2}, True, 'bucket_with_one_object'),
        ({"number_clients": 2}, False, 'bucket_with_one_object'),
    ],
    indirect=['multiple_s3_clients', 'bucket_with_one_object'],  # Passa as fixtures para os parâmetros
)
def test_boto_delete_bucket_force(multiple_s3_clients, bucket_with_one_object, force):
    s3_owner = multiple_s3_clients[0]
    s3_other = multiple_s3_clients[1]
    
    bucket_name = bucket_with_one_object[0]

    if force:
        try:
            event_system = s3_owner.meta.events
            event_system.register_first('before-send.s3.*', lambda request, **kwargs: _add_header(request, force))

            s3_owner.delete_bucket(Bucket=bucket_name)

            response = s3_owner.list_buckets()
            assert bucket_name not in [b['Name'] for b in response['Buckets']]

            with pytest.raises(botocore.exceptions.ClientError) as excinfo:
                s3_owner.delete_bucket(Bucket=bucket_name)
            assert "NoSuchBucket" in str(excinfo.value)

            with pytest.raises(botocore.exceptions.ClientError) as excinfo:
                s3_other.create_bucket(Bucket=bucket_name)
            assert "BucketAlreadyExists" in str(excinfo.value)

        except botocore.exceptions.ClientError as e:
            pytest.fail(f"Erro ao deletar bucket com 'force=True': {e}")

    else:
        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            s3_owner.delete_bucket(Bucket=bucket_name)
        assert "BucketNotEmpty" in str(excinfo.value)  # O erro esperado é BucketNotEmpty

        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            s3_other.create_bucket(Bucket=bucket_name)
        assert "BucketAlreadyExists" in str(excinfo.value)

run_example(__name__, "test_boto_delete_bucket_force", config=config)
