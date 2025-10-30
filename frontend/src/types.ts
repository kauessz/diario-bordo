export type KPIs = {
  client: string
  ym: string
  total_operacoes: number
  porto_top: string | null
  porto_top_qtd: number
  porto_low: string | null
  porto_low_qtd: number
  atrasos: {
    coleta: number
    entrega: number
    por_motivo: Record<string, number>
    por_local: Record<string, number>
  }
  reagendamentos_total: number
}
