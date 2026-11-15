using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;

namespace NifiKit.Tests.Integration;

/// <summary>
///   Just enough of NiFi's REST API to set up and inspect a Site-to-Site test
///   flow (input port -> connection -> funnel) as the initial admin.
/// </summary>
internal sealed class NifiAdminClient {
  private const string CLIENT_ID = "nifikit-integration-tests";
  private readonly HttpClient http_;

  public NifiAdminClient(HttpClient http_client, Uri api_base) {
    http_ = http_client;
    http_.BaseAddress = api_base;
  }

  public async Task<string> GetIdentityAsync() {
    JsonNode user = await GetJsonAsync("flow/current-user");
    return user["identity"]!.GetValue<string>();
  }

  public async Task<string> GetRootGroupIdAsync() {
    // Returns the id even before the admin has root group permissions
    JsonNode flow = await GetJsonAsync("flow/process-groups/root");
    return flow["processGroupFlow"]!["id"]!.GetValue<string>();
  }

  public async Task<string> GetUserIdAsync(string identity) {
    JsonNode users = await GetJsonAsync("tenants/users");
    return users["users"]!.AsArray()
                          .First(u => u!["component"]!["identity"]!
                                        .GetValue<string>() ==
                                      identity)!["id"]!
                          .GetValue<string>();
  }

  /// <summary>
  ///   Ensures <paramref name="user_id" /> is part of the policy for the action
  ///   on the resource, creating the policy when it does not exist yet.
  /// </summary>
  public async Task GrantAsync(string action, string resource, string user_id) {
    using HttpResponseMessage existing =
      await http_.GetAsync($"policies/{action}{resource}");
    if (existing.IsSuccessStatusCode) {
      JsonNode policy = (await existing.Content.ReadFromJsonAsync<JsonNode>())!;
      JsonNode component = policy["component"]!;
      // GET returns the inherited policy when there is no exact one
      if (component["resource"]!.GetValue<string>() == resource) {
        JsonArray users = component["users"]!.AsArray();
        if (users.Any(u => u!["id"]!.GetValue<string>() == user_id)) {
          return;
        }

        JsonArray user_ids = new();
        foreach (JsonNode? user in users) {
          user_ids.Add(new JsonObject { ["id"] = user!["id"]!.GetValue<string>() });
        }

        user_ids.Add(new JsonObject { ["id"] = user_id });
        await SendJsonAsync(
          HttpMethod.Put,
          $"policies/{policy["id"]!.GetValue<string>()}",
          new JsonObject {
            ["revision"] = policy["revision"]!.DeepClone(),
            ["component"] = new JsonObject {
              ["id"] = policy["id"]!.GetValue<string>(),
              ["users"] = user_ids
            }
          }
        );
        return;
      }
    } else if (existing.StatusCode != HttpStatusCode.NotFound) {
      existing.EnsureSuccessStatusCode();
    }

    await SendJsonAsync(
      HttpMethod.Post,
      "policies",
      new JsonObject {
        ["revision"] = new JsonObject { ["version"] = 0 },
        ["component"] = new JsonObject {
          ["action"] = action,
          ["resource"] = resource,
          ["users"] = new JsonArray(new JsonObject { ["id"] = user_id })
        }
      }
    );
  }

  public async Task<string> CreateInputPortAsync(string group_id, string name) {
    JsonNode port = await SendJsonAsync(
                      HttpMethod.Post,
                      $"process-groups/{group_id}/input-ports",
                      NewComponent(new JsonObject {
                        ["name"] = name,
                        ["position"] = new JsonObject { ["x"] = 0, ["y"] = 0 }
                      })
                    );
    return port["id"]!.GetValue<string>();
  }

  public async Task<string> CreateFunnelAsync(string group_id) {
    JsonNode funnel = await SendJsonAsync(
                        HttpMethod.Post,
                        $"process-groups/{group_id}/funnels",
                        NewComponent(new JsonObject {
                          ["position"] = new JsonObject { ["x"] = 0, ["y"] = 300 }
                        })
                      );
    return funnel["id"]!.GetValue<string>();
  }

  public async Task<string> CreateConnectionAsync(string group_id,
                                                  string port_id,
                                                  string funnel_id) {
    JsonNode connection = await SendJsonAsync(
                            HttpMethod.Post,
                            $"process-groups/{group_id}/connections",
                            NewComponent(new JsonObject {
                              ["source"] = new JsonObject {
                                ["id"] = port_id,
                                ["groupId"] = group_id,
                                ["type"] = "INPUT_PORT"
                              },
                              ["destination"] = new JsonObject {
                                ["id"] = funnel_id,
                                ["groupId"] = group_id,
                                ["type"] = "FUNNEL"
                              }
                            })
                          );
    return connection["id"]!.GetValue<string>();
  }

  public async Task SetPortStateAsync(string port_id, string state) {
    JsonNode port = await GetJsonAsync($"input-ports/{port_id}");
    await SendJsonAsync(
      HttpMethod.Put,
      $"input-ports/{port_id}/run-status",
      new JsonObject {
        ["revision"] = WithClientId(port["revision"]!),
        ["state"] = state
      }
    );
  }

  /// <summary>
  ///   Lists the FlowFiles queued in a connection with their attributes and
  ///   content.
  /// </summary>
  public async Task<List<QueuedFlowFile>> GetQueuedFlowFilesAsync(
    string connection_id) {
    JsonNode request = await SendJsonAsync(
                         HttpMethod.Post,
                         $"flowfile-queues/{connection_id}/listing-requests",
                         null
                       );
    string request_id = request["listingRequest"]!["id"]!.GetValue<string>();
    JsonNode listing = request;
    for (int i = 0; i < 50 &&
                    !listing["listingRequest"]!["finished"]!.GetValue<bool>();
         i++) {
      await Task.Delay(100);
      listing = await GetJsonAsync(
                  $"flowfile-queues/{connection_id}/listing-requests/{request_id}"
                );
    }

    await http_.DeleteAsync(
      $"flowfile-queues/{connection_id}/listing-requests/{request_id}"
    );

    List<QueuedFlowFile> result = new();
    foreach (JsonNode? summary in listing["listingRequest"]!["flowFileSummaries"]!
                                    .AsArray()) {
      string uuid = summary!["uuid"]!.GetValue<string>();
      JsonNode details =
        await GetJsonAsync($"flowfile-queues/{connection_id}/flowfiles/{uuid}");
      Dictionary<string, string> attributes = new();
      foreach ((string key, JsonNode? value) in
               details["flowFile"]!["attributes"]!.AsObject()) {
        attributes[key] = value?.GetValue<string>() ?? "";
      }

      byte[] content = await http_.GetByteArrayAsync(
                         $"flowfile-queues/{connection_id}/flowfiles/{uuid}/content"
                       );
      result.Add(new QueuedFlowFile(attributes, content));
    }

    return result;
  }

  /// <summary>
  ///   Best-effort removal of the test flow.
  /// </summary>
  public async Task DeleteFlowAsync(string? port_id, string? connection_id,
                                    string? funnel_id) {
    async Task Try(Func<Task> action) {
      try {
        await action();
      } catch (Exception) {
        // Leave remnants rather than hide the real test failure
      }
    }

    if (port_id != null) {
      await Try(() => SetPortStateAsync(port_id, "STOPPED"));
    }

    if (connection_id != null) {
      await Try(async () => {
        JsonNode drop = await SendJsonAsync(
                          HttpMethod.Post,
                          $"flowfile-queues/{connection_id}/drop-requests",
                          null
                        );
        string drop_id = drop["dropRequest"]!["id"]!.GetValue<string>();
        for (int i = 0; i < 50 &&
                        !drop["dropRequest"]!["finished"]!.GetValue<bool>();
             i++) {
          await Task.Delay(100);
          drop = await GetJsonAsync(
                   $"flowfile-queues/{connection_id}/drop-requests/{drop_id}"
                 );
        }

        await http_.DeleteAsync(
          $"flowfile-queues/{connection_id}/drop-requests/{drop_id}"
        );
      });
      await Try(() => DeleteComponentAsync("connections", connection_id));
    }

    if (port_id != null) {
      await Try(() => DeleteComponentAsync("input-ports", port_id));
    }

    if (funnel_id != null) {
      await Try(() => DeleteComponentAsync("funnels", funnel_id));
    }
  }

  private async Task DeleteComponentAsync(string type, string id) {
    JsonNode entity = await GetJsonAsync($"{type}/{id}");
    long version = entity["revision"]!["version"]!.GetValue<long>();
    using HttpResponseMessage response = await http_.DeleteAsync(
                                           $"{type}/{id}?version={version}&clientId={CLIENT_ID}"
                                         );
    response.EnsureSuccessStatusCode();
  }

  private static JsonObject NewComponent(JsonObject component) {
    return new JsonObject {
      ["revision"] = new JsonObject {
        ["version"] = 0,
        ["clientId"] = CLIENT_ID
      },
      ["component"] = component
    };
  }

  private static JsonNode WithClientId(JsonNode revision) {
    JsonNode copy = revision.DeepClone();
    copy["clientId"] = CLIENT_ID;
    return copy;
  }

  private async Task<JsonNode> GetJsonAsync(string path) {
    using HttpResponseMessage response = await http_.GetAsync(path);
    await EnsureSuccessAsync(response, "GET " + path);
    return (await response.Content.ReadFromJsonAsync<JsonNode>())!;
  }

  private async Task<JsonNode> SendJsonAsync(HttpMethod method, string path,
                                             JsonNode? body) {
    using HttpRequestMessage request = new(method, path);
    if (body != null) {
      request.Content = JsonContent.Create(body);
    }

    using HttpResponseMessage response = await http_.SendAsync(request);
    await EnsureSuccessAsync(response, $"{method} {path}");
    return (await response.Content.ReadFromJsonAsync<JsonNode>())!;
  }

  private static async Task EnsureSuccessAsync(HttpResponseMessage response,
                                               string action) {
    if (!response.IsSuccessStatusCode) {
      string body = await response.Content.ReadAsStringAsync();
      throw new InvalidOperationException(
        $"{action} failed: {(int)response.StatusCode} {body}"
      );
    }
  }
}

internal sealed record QueuedFlowFile(Dictionary<string, string> attributes,
                                      byte[] content);
