const SqlConn = require('tedious').Connection;
const SqlReq = require('tedious').Request;
const fetch = require('node-fetch');
const csv = require('csvtojson');
const options = require('./config');

const chave_api = options.chave_api
const db_config = options.db_config
const api_url = options.api_url
const linx_grupo = options.linx_grupo

let all_funcs
let cnpjs = []
let executions = []
let dates = [];
//const from = new Date('2019-01-01') // inclusive
//const to = new Date('2019-03-29') // inclusive 
const from = new Date((new Date()).getTime() - (1000*60*60*24))
const to = new Date((new Date()).getTime() - (1000*60*60*24))

let constructBody = async (APIname='', parameters={}) => {
  let body_gp = `<?xml version=\"1.0\" encoding=\"UTF-8\"?>
    <LinxMicrovix><Authentication user=\"linx_export\" password=\"linx_export\" />
    <ResponseFormat>csv</ResponseFormat><Command><Name>${APIname}</Name><Parameters>`
  for (const param of Object.keys(parameters)) {
    body_gp += `<Parameter id=\"${param}\">${parameters[param]}</Parameter>`
  }
  body_gp += "</Parameters></Command></LinxMicrovix>"
  return body_gp
}

// GET DATA
var getData = async (APIname='', parameters={}, insertFunction, deleteStr, deleteAfter=false, deleteAfter2) => {
  //process.stdout.write(APIname + ' - '+ parameters.cnpjEmp+' - ')
  let body_gp = await constructBody(APIname, parameters)
  
  let resp = await fetch(api_url, {
    method: 'post',
    body:    body_gp,
    headers: {'User-Agent': 'request'},
  })
  
  let resp_csv = await resp.text()
  let csv_array = await csv().fromString(resp_csv.substring(6))
  
  if( APIname === 'LinxGrupoLojas'){
    csv_array.forEach((json, idx)=>{
      if(json.CNPJ) cnpjs.push(json.CNPJ);
      if((idx+1)>=csv_array.length) start();
    });
  }else{
    
    let dia = new Date(parameters.data_inicial)
    let string_yest = ((dia.getDate().toString().length ===1) ? "0" : "")+dia.getDate()+"/"
                      +(((dia.getMonth()+1).toString().length ===1) ? "0" : "")
                      +(dia.getMonth()+1)+"/"+dia.getFullYear()
    let new_csv_array = csv_array.filter( json => json.data_lancamento ? // retira dados do dia seguinte
                                         (json.data_lancamento.substr(0, 10) === string_yest) : true ) 
    
    let deleted = await insert("delete from "+APIname.replace('Linx', '')+" where "+deleteStr, APIname)
    if(!deleted){console.log(APIname.replace('Linx', ''), deleteStr)}
    
    let sql_del = '';
    if(csv_array.length === 0){
      process.stdout.write('.')
      next()
    }else{
      process.stdout.write('filtered: '+(csv_array.length - new_csv_array.length)+' - ')
      //process.stdout.write(' filtered: '+new_csv_array.length+' ')
      process.stdout.write(csv_array.length+' - '+parameters.data_inicial)
      console.time(APIname);
      let values = [];
      
      new_csv_array.forEach((a,b)=>{
        values = insertFunction(a,b,values)
      });
      
      for(let idx = 0; idx < values.length; idx++){
        let dt = values[idx]
        let inserted = await insert("insert into "+APIname.replace('Linx', '')+" values "+dt.substr(1, dt.length-1)+";")
        process.stdout.write(' ~inserting~ ');
        if(!inserted){console.log(dt)}
      }
      if(deleteAfter){
        let deleted2 = await insert(deleteAfter)
        if(!deleted2){console.log(deleteAfter)}
      }
      if(deleteAfter2){
        let deleted2 = await insert("delete from "+APIname.replace('Linx', '')+" where "+deleteAfter2)
        if(!deleted2){console.log(deleteAfter2)}
      }
      
      console.timeEnd(APIname)
      next()
    }
  }
}

const to2 = (new Date(to.getTime()+(1000*60*60*24)))

for(let dia = new Date(from); dia.getTime() < to2.getTime(); dia.setDate(dia.getDate()+1)){
  let string_yest = dia.getFullYear()+"-"+(((dia.getMonth()+1).toString().length ===1) ? "0" : "")+(dia.getMonth()+1)+"-"+((dia.getDate().toString().length ===1) ? "0" : "")+dia.getDate();
  let next = new Date(dia)
  next.setDate(dia.getDate()+1)
  let string_next = next.getFullYear()+"-"+(((next.getMonth()+1).toString().length ===1) ? "0" : "")+(next.getMonth()+1)+"-"+((next.getDate().toString().length ===1) ? "0" : "")+next.getDate();
  dates.push([string_yest, string_next, new Date(dia)]);
}

dates.reverse()

all_funcs = ['LinxClientesFornec', 'LinxLojas', 'LinxMovimento', 'LinxPlanos', 'LinxMovimentoPlanos',
  'LinxMovimentoAcoesPromocionais', 'LinxVendedores', 'LinxAcoesPromocionais', 'LinxProdutos',
  'LinxProdutosInventario', 'LinxProdutosDetalhes']

var next = function(){
  let fn = executions.pop();
  if(fn){
    getData( fn[0], fn[1], fn[2], fn[3], fn[4], fn[5] )
  }
}

var start = function(){
  dates.forEach( dia => {
    cnpjs.forEach( cnpj => { // rodar cnpj antes para tabelas que não possuem data
      all_funcs.forEach( apiName => {
        let string_yest = ((dia[2].getDate().toString().length ===1) ? "0" : "")+dia[2].getDate()+"/"+(((dia[2].getMonth()+1).toString().length ===1) ? "0" : "")+(dia[2].getMonth()+1)+"/"+dia[2].getFullYear()
        let params = {chave: chave_api, cnpjEmp: cnpj};
        let insertFunction; 
        let deleteStr;
        let deleteAfter = false;
        let deleteAfter2 = false;
        switch(apiName){
          case 'LinxProdutosInventario':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='') ? 'null' : json.portal.replace(',','.'))+','+'\''+json.cnpj_emp+'\','+((json.cod_produto==='') ? 'null' : json.cod_produto.replace(',','.'))+','+'\''+json.cod_barra+'\','+((json.quantidade==='') ? 'null' : json.quantidade.replace(',','.'))+')'
              return values;
            }
            params = {...params, data_inventario: dia[0]}
            
          break;
          case 'LinxProdutosDetalhes':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}'`;
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='') ? 'null' : json.portal.replace(',','.'))+','+'\''+json.cnpj_emp+'\','+((json.cod_produto==='') ? 'null' : json.cod_produto.replace(',','.'))+','+'\''+json.cod_barra+'\','+((json.quantidade==='') ? 'null' : json.quantidade.replace(',','.'))+','+((json.preco_custo==='') ? 'null' : json.preco_custo.replace(',','.'))+','+((json.preco_venda==='') ? 'null' : json.preco_venda.replace(',','.'))+','+((json.custo_medio==='') ? 'null' : json.custo_medio.replace(',','.'))+','+((json.id_config_tributaria==='') ? 'null' : json.id_config_tributaria.replace(',','.'))+','+'\''+json.desc_config_tributaria+'\','+((json.despesas1==='') ? 'null' : json.despesas1.replace(',','.'))+')';
              return values;
            }
            params = {...params, data_mov_ini: 'NULL', data_mov_fim: 'NULL', cod_produto: 'NULL', referencia: 'NULL'}
            
          break;
          case 'LinxProdutos':
            deleteStr = `cnpj = '${params.cnpjEmp}' and convert(date, dt_update, 103) = '${dia[0]}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='') ? 'null' : json.portal.replace(',','.')) +','+((json.cod_produto==='') ? 'null' : json.cod_produto.replace(',','.')) +','+'\''+json.cod_barra+'\','+'\''+json.nome+'\','+'\''+json.ncm+'\','+'\''+json.cest+'\','+'\''+json.referencia+'\','+'\''+json.cod_auxiliar+'\','+'\''+json.unidade+'\','+'\''+json.desc_cor+'\','+'\''+json.desc_tamanho+'\','+'\''+json.desc_setor+'\','+'\''+json.desc_linha+'\','+'\''+json.desc_marca+'\','+'\''+json.desc_colecao+'\','+'\''+json.dt_update+'\','+((json.cod_fornecedor==='') ? 'null' : json.cod_fornecedor.replace(',','.')) +','+'\''+json.desativado+'\','+'\''+json.desc_espessura+'\','+((json.id_espessura==='') ? 'null' : json.id_espessura.replace(',','.')) +','+'\''+json.desc_classificacao+'\','+((json.id_classificacao==='') ? 'null' : json.id_classificacao.replace(',','.')) +','+((json.origem_mercadoria==='') ? 'null' : json.origem_mercadoria.replace(',','.')) +','+((json.peso_liquido==='') ? 'null' : json.peso_liquido.replace(',','.')) +','+((json.peso_bruto==='') ? 'null' : json.peso_bruto.replace(',','.')) +','+((json.id_cor==='') ? 'null' : json.id_cor.replace(',','.')) +','+((json.id_tamanho==='') ? 'null' : json.id_tamanho.replace(',','.')) +','+((json.id_setor==='') ? 'null' : json.id_setor.replace(',','.')) +','+((json.id_linha==='') ? 'null' : json.id_linha.replace(',','.')) +','+((json.id_marca==='') ? 'null' : json.id_marca.replace(',','.')) +','+((json.id_colecao==='') ? 'null' : json.id_colecao.replace(',','.')) +','+'\''+json.dt_inclusao+'\', \''+cnpj+'\')';
              return values;
            }
            deleteAfter = `delete from Produtos where id in (
                              select min(id) from Produtos b 
                              group by portal, cod_produto, cod_barra, nome, Ncm, cest, referencia, cod_auxiliar, unidade, desc_cor, desc_tamanho, desc_setor, desc_linha, desc_marca, desc_colecao, dt_update, cod_fornecedor, desativado, desc_espessura, id_espessura, desc_classificacao, id_classificacao, origem_mercadoria, peso_liquido, peso_bruto, id_cor, id_tamanho, id_setor, id_linha, id_marca, id_colecao, dt_inclusao, cnpj
                              having count(1) > 1
                           )`
            params = {...params, id_setor: 'NULL', id_linha: 'NULL', id_marca: 'NULL', id_colecao: 'NULL', dt_update_inicio: dia[0], dt_update_fim: dia[1]}
          break;
          case 'LinxAcoesPromocionais':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+json.portal.replace(',','.')+','+'\''+json.cnpj_emp+'\','+json.id_acoes_promocionais.replace(',','.')+','+'\''+json.descricao+'\','+'\''+json.vigencia_inicio+'\','+'\''+json.vigencia_fim+'\','+'\''+json.observacao+'\','+json.ativa.replace(',','.')+','+json.excluida.replace(',','.')+','+json.integrada.replace(',','.')+','+json.qtde_integrada.replace(',','.')+','+json.valor_pago_franqueadora.replace(',','.')+')';
              return values;
            }
            params = {...params, ativa: 1, data_inicial: 'NULL', data_fim: 'NULL', integrada: 'NULL'}
            
          break;
          case 'LinxVendedores':
            deleteStr = `cnpj = '${params.cnpjEmp}' and convert(date, dt_upd, 103) = '${dia[0]}';`;
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='')? '' : json.portal).replace(',','.')+','+((json.cod_vendedor==='')? '' : json.cod_vendedor).replace(',','.')+','+'\''+((json.nome_vendedor==='')? '' : json.nome_vendedor)+'\','+'\''+((json.tipo_vendedor==='')? '' : json.tipo_vendedor)+'\','+'\''+((json.end_vend_rua==='')? '' : json.end_vend_rua)+'\','+((json.end_vend_numero==='')? '' : json.end_vend_numero).replace(',','.')+','+'\''+((json.end_vend_complemento==='')? '' : json.end_vend_complemento)+'\','+'\''+((json.end_vend_bairro==='')? '' : json.end_vend_bairro)+'\','+'\''+((json.end_vend_cep==='')? '' : json.end_vend_cep)+'\','+'\''+((json.end_vend_cidade==='')? '' : json.end_vend_cidade)+'\','+'\''+((json.end_vend_uf==='')? '' : json.end_vend_uf)+'\','+'\''+((json.fone_vendedor==='')? '' : json.fone_vendedor)+'\','+'\''+((json.mail_vendedor==='')? '' : json.mail_vendedor)+'\','+'\''+((json.dt_upd==='')? '' : json.dt_upd)+'\','+'\''+((json.cpf_vendedor==='')? '' : json.cpf_vendedor)+'\','+'\''+((json.ativo==='')? '' : json.ativo)+'\','+'\''+((json.data_admissao==='')? '' : json.data_admissao)+'\','+'\''+((json.data_saida==='')? '' : json.data_saida)+'\', \''+cnpj+'\')';
              return values;
            }
            params = {...params, data_upd_inicial: dia[0], data_upd_fim: dia[1]}
            
          break;
          case 'LinxMovimentoAcoesPromocionais':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='')? 'null' : json.portal).replace(',','.')+','+'\''+((json.cnpj_emp==='')? 'null' : json.cnpj_emp)+'\','+'\''+((json.identificador==='')? 'null' : json.identificador)+'\','+((json.transacao==='')? 'null' : json.transacao).replace(',','.')+','+((json.id_acoes_promocionais==='')? 'null' : json.id_acoes_promocionais).replace(',','.')+','+((json.desconto_item==='')? 'null' : json.desconto_item).replace(',','.')+','+((json.quantidade==='')? 'null' : json.quantidade).replace(',','.')+')';
              return values;
            }
            params = {...params, identificador: 'NULL', data_inicial: dia[0], data_fim: dia[1]}
            
          break;
          case 'LinxMovimentoPlanos':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}' and convert(date, dt_update, 103) = '${dia[0]}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='')? 'null' : json.portal).replace(',','.')+','+'\''+((json.cnpj_emp==='')? 'null' : json.cnpj_emp)+'\','+'\''+((json.identificador==='')? 'null' : json.identificador)+'\','+((json.plano==='')? 'null' : json.plano).replace(',','.')+','+'\''+((json.desc_plano==='')? 'null' : json.desc_plano)+'\','+((json.total==='')? 'null' : json.total).replace(',','.')+','+((json.qtde_parcelas==='')? 'null' : json.qtde_parcelas).replace(',','.')+','+'\''+((json.indice_plano==='')? 'null' : json.indice_plano)+'\','+((json.cod_forma_pgto==='')? 'null' : json.cod_forma_pgto).replace(',','.')+','+'\''+((json.forma_pgto==='')? 'null' : json.forma_pgto)+'\','+'\''+((json.tipo_transacao==='')? 'null' : json.tipo_transacao)+'\','+((json.taxa_financeira==='')? 'null' : json.taxa_financeira).replace(',','.')+',\''+string_yest+'\')';
              return values;
            }
            params = {...params, identificador: 'NULL', data_inicial: dia[0], data_fim: dia[1], hora_inicial: 'NULL', hora_fim: 'NULL'}
            
          break;
          case 'LinxPlanos':
            deleteStr = `cnpj = '${params.cnpjEmp}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += ',('+((json.portal==='') ? 'null' : json.portal.replace(',','.'))+','+((json.plano==='') ? 'null' : json.plano.replace(',','.'))+','+'\''+json.desc_plano+'\','+((json.qtde_parcelas==='') ? 'null' : json.qtde_parcelas.replace(',','.'))+','+((json.prazo_entre_parcelas==='') ? 'null' : json.prazo_entre_parcelas.replace(',','.'))+','+'\''+json.tipo_plano+'\','+((json.indice_plano==='') ? 'null' : json.indice_plano.replace(',','.'))+','+((json.cod_forma_pgto==='') ? 'null' : json.cod_forma_pgto.replace(',','.'))+','+'\''+json.forma_pgto+'\','+((json.conta_central==='') ? 'null' : json.conta_central.replace(',','.'))+','+'\''+json.tipo_transacao+'\','+((json.taxa_financeira==='') ? 'null' : json.taxa_financeira.replace(',','.'))+','+'\''+json.dt_upd+'\','+'\''+json.desativado+'\','+'\''+json.usa_tef+'\','+'\''+cnpj+'\')';
              return values;
            }
            params = {...params, data_upd_inicial: dia[0], data_upd_fim: dia[1]}
          break;
          case 'LinxLojas':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += `,(${json.portal.replace(',', '.')||null}, ${json.empresa.replace(',', '.')||null}, \'${json.nome_emp.replace('\'', '\'\'')}\', \'${json.razao_emp.replace('\'', '\'\'')}\', \'${json.cnpj_emp.replace('\'', '\'\'')}\', \'${json.inscricao_emp.replace('\'', '\'\'')}\', \'${json.endereco_emp.replace('\'', '\'\'')}\', ${json.num_emp.replace(',', '.')||null}, \'${json.complement_emp.replace('\'', '\'\'')}\', \'${json.bairro_emp.replace('\'', '\'\'')}\', \'${json.cep_emp.replace('\'', '\'\'')}\', \'${json.cidade_emp.replace('\'', '\'\'')}\', \'${json.estado_emp.replace('\'', '\'\'')}\', \'${json.fone_emp.replace('\'', '\'\'')}\', \'${json.email_emp.replace('\'', '\'\'')}\', ${json.cod_ibge_municipio.replace(',', '.')||null}, \'${json.data_criacao_emp.replace('\'', '\'\'')}\', \'${json.data_criacao_portal.replace('\'', '\'\'')}\', \'${json.sistema_tributacao.replace('\'', '\'\'')}\', \'${json.regime_tributario.replace('\'', '\'\'')}\', \'${json.area_empresa.replace('\'', '\'\'')}\')`
              return values;
            }
            params = {...params}
          break;
          case 'LinxClientesFornec':
            deleteStr = `cnpj = '${params.cnpjEmp}' and convert(date, dt_update, 103) = '${dia[0]}';`
            insertFunction = (json, num, values) => {
              values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
              values[Math.floor(num/1000)] += `,(${json.portal.replace(',', '.')||null}, ${json.cod_cliente.replace(',', '.')||null}, \'${json.razao_cliente.replace('\'', '\'\'')}\', \'${json.nome_cliente.replace('\'', '\'\'')}\', \'${json.doc_cliente.replace('\'', '\'\'')}\', \'${json.tipo_cliente.replace('\'', '\'\'')}\', \'${json.endereco_cliente.replace('\'', '\'\'')}\', \'${json.numero_rua_cliente.replace('\'', '\'\'')}\', \'${json.complement_end_cli.replace('\'', '\'\'')}\', \'${json.bairro_cliente.replace('\'', '\'\'')}\', \'${json.cep_cliente.replace('\'', '\'\'')}\', \'${json.cidade_cliente.replace('\'', '\'\'')}\', \'${json.uf_cliente.replace('\'', '\'\'')}\', \'${json.pais.replace('\'', '\'\'')}\', \'${json.fone_cliente.replace('\'', '\'\'')}\', \'${json.email_cliente.replace('\'', '\'\'')}\', \'${json.sexo.replace('\'', '\'\'')}\', \'${json.data_cadastro.replace('\'', '\'\'')}\', \'${json.data_nascimento.replace('\'', '\'\'')}\', \'${json.cel_cliente.replace('\'', '\'\'')}\', \'${json.ativo.replace('\'', '\'\'')}\', \'${json.dt_update.replace('\'', '\'\'')}\', \'${json.inscricao_estadual.replace('\'', '\'\'')}\', \'${json.incricao_municipal.replace('\'', '\'\'')}\', \'${json.identidade_cliente.replace('\'', '\'\'')}\', \'${json.cartao_fidelidade.replace('\'', '\'\'')}\', ${json.cod_ibge_municipio.replace(',', '.')||null}, \'${json.classe_cliente.replace('\'', '\'\'')}\', \'${json.matricula_conveniado.replace('\'', '\'\'')}\', \'${json.tipo_cadastro.replace('\'', '\'\'')}\', ${json.id_estado_civil.replace(',', '.')||null}, \'${json.fax_cliente.replace('\'', '\'\'')}\', \'${json.site_cliente.replace('\'', '\'\'')}\', \'${cnpj.replace('\'', '\'\'')}\')`
              return values;
            }
            params = {...params, data_inicial: dia[0], data_fim: dia[1]}
          break;
          case 'LinxMovimento':
            deleteStr = `cnpj_emp = '${params.cnpjEmp}' and convert(date, dt_update, 103) = '${dia[0]}';`
            insertFunction = (json, num, values) => {
              //console.log(JSON.stringify(json.dt_update+' - '+ json.data_lancamento))
              if(json.excluido === 'N'){
                values[Math.floor(num/1000)] ? true : values[Math.floor(num/1000)]='';
                values[Math.floor(num/1000)] += ',('+((json.portal!=="") ? json.portal.replace(',', '.') : 'null') +','+'\''+json.cnpj_emp+'\','+ ((json.transacao!=="") ? json.transacao.replace(',', '.') : 'null') +','+((json.usuario!=="") ? json.usuario.replace(',', '.') : 'null') +','+((json.documento!=="") ? json.documento.replace(',', '.') : 'null') +','+'\''+json.chave_nf+'\','+ ((json.ecf!=="") ? json.ecf.replace(',', '.') : 'null') +','+'\''+json.numero_serie_ecf+'\','+ ((json.modelo_nf!=="") ? json.modelo_nf.replace(',', '.') : 'null') +','+'\''+json.data_documento+'\','+ '\''+json.data_lancamento+'\','+ ((json.codigo_cliente!=="") ? json.codigo_cliente.replace(',', '.') : 'null') +','+'\''+json.serie+'\','+ '\''+json.desc_cfop+'\','+ '\''+json.id_cfop+'\','+ ((json.cod_vendedor!=="") ? json.cod_vendedor.replace(',', '.') : 'null') +','+'\''+json.quantidade+'\','+ json.preco_custo.replace(',', '.')+','+ json.valor_liquido.replace(',', '.')+','+ ((json.desconto!=="") ? json.desconto.replace(',', '.') : 'null') +','+'\''+json.cst_icms+'\','+ '\''+json.cst_pis+'\','+ '\''+json.cst_cofins+'\','+ '\''+json.cst_ipi+'\','+ ((json.valor_icms!=="") ? json.valor_icms.replace(',', '.') : 'null') +','+((json.aliquota_icms!=="") ? json.aliquota_icms.replace(',', '.') : 'null') +','+((json.base_icms!=="") ? json.base_icms.replace(',', '.') : 'null') +','+((json.valor_pis!=="") ? json.valor_pis.replace(',', '.') : 'null') +','+((json.aliquota_pis!=="") ? json.aliquota_pis.replace(',', '.') : 'null') +','+((json.base_pis!=="") ? json.base_pis.replace(',', '.') : 'null') +','+((json.valor_cofins!=="") ? json.valor_cofins.replace(',', '.') : 'null') +','+((json.aliquota_cofins!=="") ? json.aliquota_cofins.replace(',', '.') : 'null') +','+((json.base_cofins!=="") ? json.base_cofins.replace(',', '.') : 'null') +','+((json.valor_icms_st!=="") ? json.valor_icms_st.replace(',', '.') : 'null') +','+((json.aliquota_icms_st!=="") ? json.aliquota_icms_st.replace(',', '.') : 'null') +','+((json.base_icms_st!=="") ? json.base_icms_st.replace(',', '.') : 'null') +','+((json.valor_ipi!=="") ? json.valor_ipi.replace(',', '.') : 'null') +','+((json.aliquota_ipi!=="") ? json.aliquota_ipi.replace(',', '.') : 'null') +','+((json.base_ipi!=="") ? json.base_ipi.replace(',', '.') : 'null') +','+json.valor_total.replace(',', '.')+','+ '\''+json.forma_dinheiro+'\','+ ((json.total_dinheiro!=="") ? json.total_dinheiro.replace(',', '.') : 'null') +','+'\''+json.forma_cheque+'\','+ ((json.total_cheque!=="") ? json.total_cheque.replace(',', '.') : 'null') +','+'\''+json.forma_cartao+'\','+ ((json.total_cartao!=="") ? json.total_cartao.replace(',', '.') : 'null') +','+'\''+json.forma_crediario+'\','+ ((json.total_crediario!=="") ? json.total_crediario.replace(',', '.') : 'null') +','+'\''+json.forma_convenio+'\','+ ((json.total_convenio!=="") ? json.total_convenio.replace(',', '.') : 'null') +','+((json.frete!=="") ? json.frete.replace(',', '.') : 'null') +','+'\''+json.operacao+'\','+ '\''+json.tipo_transacao+'\','+ ((json.cod_produto!=="") ? json.cod_produto.replace(',', '.') : 'null') +','+'\''+json.cod_barra+'\','+ '\''+json.cancelado+'\','+ '\''+json.excluido+'\','+ '\''+json.soma_relatorio+'\','+ '\''+json.identificador+'\','+ '\''+json.deposito+'\','+ '\''+json.obs+'\','+json.preco_unitario.replace(',', '.')+','+ '\''+json.hora_lancamento+'\','+ '\''+json.natureza_operacao+'\','+ ((json.tabela_preco!=="") ? json.tabela_preco.replace(',', '.') : 'null') +','+'\''+json.nome_tabela_preco+'\','+ ((json.cod_sefaz_situacao!=="") ? json.cod_sefaz_situacao.replace(',', '.') : 'null') +','+'\''+json.desc_sefaz_situacao+'\','+ '\''+json.protocolo_aut_nfe+'\','+ '\''+json.dt_update+'\','+ '\''+json.forma_cheque_prazo+'\','+ ((json.total_cheque_prazo!=="") ? json.total_cheque_prazo.replace(',', '.') : 'null') +')';
              }else{
                deleteAfter2 += ` (cod_barra='${json.cod_barra}' and cnpj_emp='${json.cnpj_emp}' and chave_nf='${json.chave_nf}' and portal='${json.portal}' and documento = '${json.documento}' ) or`
              }
              return values;
            }
            deleteAfter = `delete from Movimento where id in (
                            select min(id) min_id from Movimento b 
                            group by b.cod_barra, b.cnpj_emp, b.chave_nf, b.portal, b.documento, b.data_lancamento, b.dt_update, b.transacao
                            HAVING count(1)> 1
                          )`
            params = {...params, data_inicial: dia[0], data_fim: dia[1]}
          break;
        }
        executions.push([apiName, params, insertFunction, deleteStr, deleteAfter, deleteAfter2]); // dia, cnpj, funcao
      });
    });
  });
  next();
};

let insert = (sql, table='') => {
  return new Promise((resolve, reject) => {
    let db_conn = new MongoConn(db_config)
    db_conn.on('connect', (err) => {if (!err){
      let req_in = new MongoReq(sql, (err, rowCount, rows) => {
        db_conn.close()
        if(err){console.log(err + ' table: '+table+' sql: '+sql)}
        else{resolve(true)}
      })
      db_conn.execSql(req_in)
    }})
  })
}

getData('LinxGrupoLojas', {chave: chave_api, grupo: linx_grupo});

