# Provisions the OpenBao database secrets engine for the dev stack
# (ADR-015 dynamic credentials). Run after:
#   docker compose --profile secrets up -d
# Idempotent: re-running recreates the role and updates the connection.
param(
    [string]$BaoAddr = "http://127.0.0.1:58200",
    [string]$BaoToken = "bao-dev-only",
    [string]$PostgresHost = "postgres",
    [string]$PostgresPort = "5432",
    [string]$Database = "fenced",
    [string]$Role = "dev-db"
)

$ErrorActionPreference = "Stop"
$headers = @{ "X-Vault-Token" = $BaoToken; "Content-Type" = "application/json" }

function Invoke-Bao([string]$Method, [string]$Path, $Body) {
    $uri = "$BaoAddr/v1/$Path"
    if ($null -eq $Body) {
        return Invoke-RestMethod -Method $Method -Uri $uri -Headers $headers
    }
    return Invoke-RestMethod -Method $Method -Uri $uri -Headers $headers -Body ($Body | ConvertTo-Json -Depth 6)
}

# Enable the database secrets engine (idempotent: re-enabling is a no-op).
try {
    Invoke-Bao "Post" "sys/mounts/database" @{ type = "database" } | Out-Null
    Write-Host "database secrets engine enabled"
} catch {
    Write-Host "database secrets engine already enabled"
}

Invoke-Bao "Put" "database/config/fenced" @{
    connection_url = "postgresql://fenced:fenced-dev-only@$PostgresHost`:$PostgresPort/$Database?sslmode=disable"
    allowed_roles  = @($Role)
    plugin_name    = "postgresql-database-plugin"
} | Out-Null
Write-Host "connection configured (postgres at $PostgresHost`:$PostgresPort)"

Invoke-Bao "Put" "database/roles/$Role" @{
    db_name = $Database
    creation_statements = @(
        "CREATE USER `"{{name}}`" WITH PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';",
        "GRANT CONNECT ON DATABASE $Database TO `"{{name}}`";",
        "GRANT USAGE ON SCHEMA public TO `"{{name}}`";",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO `"{{name}}`";"
    )
    default_ttl = "5m"
    max_ttl     = "15m"
} | Out-Null
Write-Host "role $Role provisioned (default_ttl 5m)"

$creds = Invoke-Bao "Get" "database/creds/$Role" $null
Write-Host "smoke issuance OK: user=$($creds.data.username) lease=$($creds.lease_id) ttl=$($creds.lease_duration)s"
