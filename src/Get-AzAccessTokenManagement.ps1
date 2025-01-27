Function Get-AzAccessTokenManagement
{
  <#
    .SYNOPSIS
    Get access token for connecting management.azure.com - used for REST API connectivity

    .DESCRIPTION
    Can be used under current connected user - or by Azure app connectivity with secret

    .PARAMETER AzAppId
    This is the Azure app id
        
    .PARAMETER AzAppSecret
    This is the secret of the Azure app

    .PARAMETER TenantId
    This is the Azure AD tenant id

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
    JSON-header to use in invoke-webrequest -UseBasicParsing / invoke-restmethod -UseBasicParsing commands

    .LINK
    https://github.com/KnudsenMorten/AzLogDcrIngestPS

    .EXAMPLE
    # using App
    $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                           -AzAppSecret $AzAppSecret `
                                           -TenantId $TenantId -Verbose:$Verbose

    #-------------------------------------------------------------------------------------------
    # Output
    #-------------------------------------------------------------------------------------------
    $Headers

    Name                           Value                                                                                                     
    ----                           -----                                                                                                     
    Accept                         application/json                                                                                          
    Content-Type                   application/json                                                                                          
    Authorization                  Bearer xxxxxx



    # connect using currently logged on admin
    $Headers = Get-AzAccessTokenManagement

    #Output sample
    $Headers

    Name                           Value                                                                                                     
    ----                           -----                                                                                                     
    Accept                         application/json                                                                                          
    Content-Type                   application/json                                                                                          
    Authorization                  Bearer xxxxxx
 #>

    [CmdletBinding()]
    param(
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


    If ( ($AzAppId) -and ($AzAppSecret) -and ($TenantId) )
        {
            $AccessTokenUri = 'https://management.azure.com/'
            $oAuthUri       = "https://login.microsoftonline.com/$($TenantId)/oauth2/token"
            $authBody       = [Ordered] @{
                                            resource = $AccessTokenUri
                                            client_id = $AzAppId
                                            client_secret = $AzAppSecret
                                            grant_type = 'client_credentials'
                                         }
            $authResponse = invoke-restmethod -UseBasicParsing -Method Post -Uri $oAuthUri -Body $authBody -ErrorAction Stop
            $token = $authResponse.access_token

            # Set the WebRequest headers
            $Headers = @{
                            'Content-Type' = 'application/json'
                            'Accept' = 'application/json'
                            'Authorization' = "Bearer $token"
                        }
        }
    Else
        {
            function ConvertFrom-SecureStringToPlainText {
                param (
                    [System.Security.SecureString]$SecureString
                )
                $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureString)
                $PlainText = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
                [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($BSTR)
                return $PlainText
            }

            $AccessToken = Get-AzAccessToken -ResourceUrl https://management.azure.com/ -AsSecureString -Verbose:$Verbose
            $Token = ConvertFrom-SecureStringToPlainText $AccessToken.Token

            $Headers = @{
                            'Content-Type' = 'application/json'
                            'Accept' = 'application/json'
                            'Authorization' = "Bearer $token"
                        }
        }

    Return [array]$Headers
}
