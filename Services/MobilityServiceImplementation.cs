using Grpc.Core;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace GrpcService1
{
    public class MobilityServiceImplementation : Mobility.MobilityBase
    {
        // Variáveis e inicializações necessárias
        private readonly ILogger<MobilityServiceImplementation> _logger;
        private readonly string _connectionString;
        private readonly ConnectionFactory _factory;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly ConcurrentDictionary<string, string> _subscribers; // clientId -> serviceId

        // Construtor que inicializa as variáveis de conexão com o RabbitMQ e a base de dados
        public MobilityServiceImplementation(ILogger<MobilityServiceImplementation> logger)
        {
            _logger = logger;
            _connectionString = "Server=MEEP_P\\SQLEXPRESS;Database=Sistemas_Distribuidos;Integrated Security=True;";
            _subscribers = new ConcurrentDictionary<string, string>();
            _factory = new ConnectionFactory() { HostName = "localhost" };
            _connection = _factory.CreateConnection();
            _channel = _connection.CreateModel();
            _channel.ExchangeDeclare(exchange: "task_exchange", type: "topic");
        }

        // Método para autenticar o usuário (cliente ou administrador)
        public override async Task<AuthResponse> AuthenticateUser(AuthRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql;
                string userType = request.UserType == "admin" ? "Administrador" : "Cliente";
                if (request.UserType == "admin")
                {
                    sql = "SELECT ID_Servico FROM Administrador WHERE ID_Admin = @id AND Senha = @password";
                }
                else
                {
                    sql = "SELECT ID_Servico FROM Cliente WHERE ID_Cliente = @id AND Senha = @password";
                }

                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@id", request.Id);
                    cmd.Parameters.AddWithValue("@password", request.Password);
                    var serviceId = cmd.ExecuteScalar();

                    if (serviceId != null)
                    {
                        string serviceName = GetServiceName(serviceId.ToString());
                        return new AuthResponse { Message = $"{userType} autenticado com sucesso no serviço {serviceName} ({serviceId}).", Success = true, ServiceId = serviceId.ToString() };
                    }
                    else
                    {
                        return new AuthResponse { Message = "Autenticação falhou. Verifique suas credenciais.", Success = false };
                    }
                }
            }
        }

        // Método auxiliar para obter o nome do serviço a partir do ID
        private string GetServiceName(string serviceId)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql = "SELECT Nome_Servico FROM Servico WHERE ID_Servico = @serviceId";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@serviceId", serviceId);
                    return cmd.ExecuteScalar()?.ToString() ?? "Desconhecido";
                }
            }
        }

        // Método para obter informações sobre um serviço específico
        public override async Task<ServiceResponse> GetServiceInfo(ServiceRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql = "SELECT Nome_Servico FROM Servico WHERE ID_Servico = @serviceId";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@serviceId", request.ServiceId);
                    var serviceName = cmd.ExecuteScalar()?.ToString() ?? "Desconhecido";

                    sql = "SELECT * FROM Tarefa WHERE ID_Servico = @serviceId";
                    using (SqlCommand taskCmd = new SqlCommand(sql, connection))
                    {
                        taskCmd.Parameters.AddWithValue("@serviceId", request.ServiceId);
                        using (SqlDataReader reader = taskCmd.ExecuteReader())
                        {
                            var tasks = new List<TaskInfo>();
                            while (reader.Read())
                            {
                                tasks.Add(new TaskInfo
                                {
                                    TaskId = reader["ID_Tarefa"].ToString(),
                                    Description = reader["Descricao"].ToString(),
                                    Status = reader["Estado"].ToString(),
                                    ClientId = reader["ID_Cliente"]?.ToString()
                                });
                            }

                            return new ServiceResponse { ServiceId = request.ServiceId, ServiceName = serviceName, Tasks = { tasks } };
                        }
                    }
                }
            }
        }

        // Método para alocar uma tarefa a um cliente
        public override async Task<TaskResponse> AllocateTask(TaskRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                // Verificar se o cliente já tem uma tarefa em andamento
                string checkTaskSql = "SELECT COUNT(*) FROM Tarefa WHERE ID_Cliente = @clientId AND Estado = 'Em curso'";
                using (SqlCommand checkCmd = new SqlCommand(checkTaskSql, connection))
                {
                    checkCmd.Parameters.AddWithValue("@clientId", request.ClientId);
                    var taskCount = (int)checkCmd.ExecuteScalar();
                    if (taskCount > 0)
                    {
                        return new TaskResponse { Message = "Cliente já tem uma tarefa em andamento.", Success = false };
                    }
                }

                // Alocar nova tarefa
                string sql = "UPDATE Tarefa SET Estado = 'Em curso', ID_Cliente = @clientId WHERE ID_Tarefa = @taskId AND Estado = 'Nao alocado' AND ID_Servico = @serviceId";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@clientId", request.ClientId);
                    cmd.Parameters.AddWithValue("@taskId", request.TaskId);
                    cmd.Parameters.AddWithValue("@serviceId", request.ServiceId);
                    int rowsAffected = cmd.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        return new TaskResponse { Message = "Tarefa alocada com sucesso.", Success = true };
                    }
                    else
                    {
                        return new TaskResponse { Message = "Falha na alocação da tarefa. Verifique se a tarefa está disponível e pertence ao seu serviço.", Success = false };
                    }
                }
            }
        }

        // Método para completar uma tarefa
        public override async Task<TaskResponse> CompleteTask(TaskRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                // Verificar se o cliente tem uma tarefa em andamento
                string getTaskSql = "SELECT ID_Tarefa FROM Tarefa WHERE ID_Cliente = @clientId AND Estado = 'Em curso'";
                string taskId = null;
                using (SqlCommand getTaskCmd = new SqlCommand(getTaskSql, connection))
                {
                    getTaskCmd.Parameters.AddWithValue("@clientId", request.ClientId);
                    var taskObj = getTaskCmd.ExecuteScalar();
                    if (taskObj != null)
                    {
                        taskId = taskObj.ToString();
                    }
                    else
                    {
                        return new TaskResponse { Message = "Cliente não tem nenhuma tarefa em andamento.", Success = false };
                    }
                }

                // Completar a tarefa
                string sql = "UPDATE Tarefa SET Estado = 'Concluido', ID_Cliente = NULL WHERE ID_Tarefa = @taskId AND ID_Cliente = @clientId";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@clientId", request.ClientId);
                    cmd.Parameters.AddWithValue("@taskId", taskId);
                    int rowsAffected = cmd.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        PublishTaskNotification(taskId, request.ServiceId, "Concluido");
                        return new TaskResponse { Message = "Tarefa concluída com sucesso.", Success = true };
                    }
                    else
                    {
                        return new TaskResponse { Message = "Falha na conclusão da tarefa.", Success = false };
                    }
                }
            }
        }

        // Método para subscrever notificações de tarefas
        public override async Task<SubscriptionResponse> SubscribeNotifications(SubscriptionRequest request, ServerCallContext context)
        {
            if (_subscribers.ContainsKey(request.ClientId))
            {
                return new SubscriptionResponse { Message = "Já se encontra subscrito.", Success = false };
            }

            var added = _subscribers.TryAdd(request.ClientId, request.ServiceId);
            if (added)
            {
                return new SubscriptionResponse { Message = $"Subscreveu as notificações do serviço {request.ServiceId}.", Success = true };
            }
            else
            {
                return new SubscriptionResponse { Message = "Erro na subscrição.", Success = false };
            }
        }

        // Método para desinscrever notificações de tarefas
        public override async Task<UnsubscriptionResponse> UnsubscribeNotifications(SubscriptionRequest request, ServerCallContext context)
        {
            if (!_subscribers.ContainsKey(request.ClientId))
            {
                return new UnsubscriptionResponse { Message = "Não tem nenhuma subscrição ativa.", Success = false };
            }

            var removed = _subscribers.TryRemove(request.ClientId, out _);
            if (removed)
            {
                return new UnsubscriptionResponse { Message = $"Desinscreveu as notificações do serviço {request.ServiceId}.", Success = true };
            }
            else
            {
                return new UnsubscriptionResponse { Message = "Erro na desinscrição.", Success = false };
            }
        }

        // Método para criar uma nova tarefa
        public override async Task<TaskResponse> CreateTask(TaskRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                // Buscar o maior ID de tarefa atual
                string getMaxIdSql = "SELECT MAX(ID_Tarefa) FROM Tarefa";
                using (SqlCommand cmd = new SqlCommand(getMaxIdSql, connection))
                {
                    var maxIdObj = cmd.ExecuteScalar();
                    int maxId = maxIdObj != DBNull.Value ? int.Parse(maxIdObj.ToString().Substring(1)) : 0;
                    string newTaskId = "T" + (maxId + 1).ToString("D3");

                    // Inserir a nova tarefa
                    string insertSql = "INSERT INTO Tarefa (ID_Tarefa, Descricao, Estado, ID_Servico) VALUES (@taskId, @description, 'Nao alocado', @serviceId)";
                    using (SqlCommand insertCmd = new SqlCommand(insertSql, connection))
                    {
                        insertCmd.Parameters.AddWithValue("@taskId", newTaskId);
                        insertCmd.Parameters.AddWithValue("@description", request.Description);
                        insertCmd.Parameters.AddWithValue("@serviceId", request.ServiceId);
                        int rowsAffected = insertCmd.ExecuteNonQuery();

                        if (rowsAffected > 0)
                        {
                            PublishTaskNotification(newTaskId, request.ServiceId, request.Description);
                            return new TaskResponse { Message = $"Tarefa criada com sucesso com o seguinte ID: {newTaskId}", Success = true };
                        }
                        else
                        {
                            return new TaskResponse { Message = "Falha ao criar tarefa.", Success = false };
                        }
                    }
                }
            }
        }

        // Método para remover uma tarefa
        public override async Task<TaskResponse> RemoveTask(TaskRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql = "DELETE FROM Tarefa WHERE ID_Tarefa = @taskId AND ID_Servico = @serviceId AND (Estado = 'Nao alocado' OR Estado = 'Concluido')";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@taskId", request.TaskId);
                    cmd.Parameters.AddWithValue("@serviceId", request.ServiceId);
                    int rowsAffected = cmd.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        return new TaskResponse { Message = "Tarefa removida com sucesso.", Success = true };
                    }
                    else
                    {
                        return new TaskResponse { Message = "Falha ao remover a tarefa. Certifique-se de que a tarefa pertence ao seu serviço e está em um estado removível.", Success = false };
                    }
                }
            }
        }

        // Método para associar um usuário a um serviço
        public override async Task<AssociationResponse> AssociateService(AssociationRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql;
                if (request.UserType == "admin")
                {
                    sql = "UPDATE Administrador SET ID_Servico = @serviceId WHERE ID_Admin = @id";
                }
                else
                {
                    sql = "UPDATE Cliente SET ID_Servico = @serviceId WHERE ID_Cliente = @id";
                }

                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@id", request.Id);
                    cmd.Parameters.AddWithValue("@serviceId", request.ServiceId);
                    int rowsAffected = cmd.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        return new AssociationResponse { Message = "Associação ao serviço realizada com sucesso.", Success = true };
                    }
                    else
                    {
                        return new AssociationResponse { Message = "Falha na associação ao serviço.", Success = false };
                    }
                }
            }
        }

        // Método para desassociar um usuário de um serviço
        public override async Task<DisassociationResponse> DisassociateService(DisassociationRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql;
                if (request.UserType == "admin")
                {
                    sql = "UPDATE Administrador SET ID_Servico = NULL WHERE ID_Admin = @id";
                }
                else
                {
                    sql = "UPDATE Cliente SET ID_Servico = NULL WHERE ID_Cliente = @id";
                }

                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@id", request.Id);
                    int rowsAffected = cmd.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        return new DisassociationResponse { Message = "Desassociação do serviço realizada com sucesso.", Success = true };
                    }
                    else
                    {
                        return new DisassociationResponse { Message = "Falha na desassociação do serviço.", Success = false };
                    }
                }
            }
        }

        // Método para gerenciar informações (inserir, remover, consultar ou associar) sobre motos
        public override async Task<ManageInfoResponse> ManageInfo(ManageInfoRequest request, ServerCallContext context)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql;

                switch (request.Action)
                {
                    case "inserir":
                        sql = "INSERT INTO Mota (ID_Mota, Modelo) VALUES (@id, @info)";
                        break;
                    case "remover":
                        sql = "DELETE FROM Mota WHERE ID_Mota = @id";
                        break;
                    case "consultar":
                        sql = @"
                SELECT Cliente.ID_Cliente, Cliente.ID_Mota, Mota.Modelo 
                FROM Cliente 
                INNER JOIN Mota ON Cliente.ID_Mota = Mota.ID_Mota
                UNION
                SELECT NULL AS ID_Cliente, Mota.ID_Mota, Mota.Modelo
                FROM Mota
                LEFT JOIN Cliente ON Mota.ID_Mota = Cliente.ID_Mota
                WHERE Cliente.ID_Mota IS NULL";
                        using (SqlCommand cmd = new SqlCommand(sql, connection))
                        {
                            using (SqlDataReader reader = cmd.ExecuteReader())
                            {
                                var clientMotos = new List<ClientMotoInfo>();
                                while (reader.Read())
                                {
                                    clientMotos.Add(new ClientMotoInfo
                                    {
                                        ClientId = reader["ID_Cliente"]?.ToString(), // Pode ser NULL
                                        MotoId = reader["ID_Mota"].ToString(),
                                        Modelo = reader["Modelo"].ToString()
                                    });
                                }

                                return new ManageInfoResponse
                                {
                                    Success = true,
                                    ClientMotos = { clientMotos }
                                };
                            }
                        }
                    case "associar":
                        sql = "UPDATE Cliente SET ID_Mota = @id WHERE ID_Cliente = @clientId";
                        using (SqlCommand cmd = new SqlCommand(sql, connection))
                        {
                            cmd.Parameters.AddWithValue("@id", request.Id);
                            cmd.Parameters.AddWithValue("@clientId", request.ClientId);
                            int rowsAffected = cmd.ExecuteNonQuery();

                            if (rowsAffected > 0)
                            {
                                return new ManageInfoResponse { Message = "Moto associada ao cliente com sucesso.", Success = true };
                            }
                            else
                            {
                                return new ManageInfoResponse { Message = "Falha na associação da moto ao cliente.", Success = false };
                            }
                        }
                    default:
                        return new ManageInfoResponse { Message = "Ação inválida.", Success = false };
                }

                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@id", request.Id);
                    cmd.Parameters.AddWithValue("@info", request.Info);
                    int rowsAffected = cmd.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        return new ManageInfoResponse { Message = "Operação realizada com sucesso.", Success = true };
                    }
                    else
                    {
                        return new ManageInfoResponse { Message = "Falha na operação.", Success = false };
                    }
                }
            }
        }

        // Método para publicar notificações sobre novas tarefas
        private void PublishTaskNotification(string taskId, string serviceId, string description)
        {
            var message = $"Nova tarefa para o serviço {serviceId} com ID:{taskId} - {description}";
            var body = Encoding.UTF8.GetBytes(message);
            _channel.BasicPublish(exchange: "task_exchange", routingKey: serviceId, basicProperties: null, body: body);
            _logger.LogInformation($"Notificação publicada: {message}");
        }
    }
}
