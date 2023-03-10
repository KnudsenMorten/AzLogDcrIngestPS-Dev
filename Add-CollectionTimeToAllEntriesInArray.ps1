Function Add-CollectionTimeToAllEntriesInArray
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    [datetime]$CollectionTime = ( Get-date ([datetime]::Now.ToUniversalTime()) -format "yyyy-MM-ddTHH:mm:ssK" )

    Write-Verbose "  Adding CollectionTime to all entries in array .... please wait !"

    $IntermediateObj = @()
    ForEach ($Entry in $Data)
        {
            $Entry | Add-Member -MemberType NoteProperty -Name CollectionTime -Value $CollectionTime -Force | Out-Null

            $IntermediateObj += $Entry
        }

    return [array]$IntermediateObj
}
