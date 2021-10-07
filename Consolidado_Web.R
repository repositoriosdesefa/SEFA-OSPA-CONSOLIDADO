####################################################################-
#---- Observatorio de Solución de Problemas Ambientales (Web)   #----
####################################################################-


#------------------ I. Librerías, drivers y directorio ----------------------

### I.1 Librerías -----------------------------------------------------------

#intall.packages("googledrive")
library(googledrive)
#intall.packages("googlesheets4")
library(googlesheets4)
#intall.packages("dplyr")
library(dplyr)
#intall.packages("lubridate")
library(lubridate)
#intall.packages("purrr")
library(purrr)
#intall.packages("bizdays")
library(bizdays)
#intall.packages("stringi")
library(stringi)



### I.2 Drivers ====

##### i) Google----
correo_usuario <- ""
drive_auth(email = correo_usuario) 
gs4_auth(token = drive_auth(email = correo_usuario), 
         email = correo_usuario)



### I.3 Directorio y fuentes de Datos ====



##### i) Directorio General y Diccionario de Variables----

dirOPA_drive <- ""

# ii) Bases de datos para el consolidado

drive_consolidado <- ""

# iii) Fuentes de datos - Sede Central

SC_RG_ID <- ""
ACT_2020 <- ""
ACT_2021 <- ""
SEG_2020 <- ""
SEG_2021 <- ""

# iv) Fuentes de datos auxiliares

# Lista de EFA
EFA_lista <- ""
# Directorio de EFA
EFA_dir <- ""
# Feriados
feriados_maestro <- ""

# v) Definición de calendario

feriados <- read_sheet(ss = feriados_maestro,
                       sheet = "Feriados")
cal_peru <- create.calendar("Perú/OEFA", 
                            feriados$FERIADOS, 
                            weekdays=c("saturday", "sunday"))




### I.4 Funciones y parámetros ====



##### i) Consolidado de ODES----

bajar_apilar_OD <- function(id, dataset, tabla){
  if(tabla == "SEGUIMIENTO") { 
    base_OD <- read_sheet(ss = id,
                          sheet = tabla,
                          skip = 2) # Salto diferente para el seguimiento
  } else {
    base_OD <- read_sheet(ss = id,
                          sheet = tabla)
  }
  # Selección de datos para el análisis
  # Diccionario
  diccionario_OD <- filter(dicOPA, 
                           DATASET == dataset,
                           TABLA == tabla,
                           `RELEVANTE PARA LA WEB` == "SI")
  cabecera_OD <- as.list(diccionario_OD)
  # Filtro de variables relevantes
  base_de_datos <- subset(base_OD, 
                           select = cabecera_OD$`CAMPO EN BD`)
  colnames(base_de_datos) <- cabecera_OD$`CODIGO CAMPO`
  base_de_datos
}

##### ii) Función robustecida de Consolidado de ODES----
R_bajar_apilar_OD <- function(id, dataset, tabla){
  out = tryCatch(bajar_apilar_OD(id, dataset, tabla),
                 error = function(e){
                   bajar_apilar_OD(id, dataset, tabla) 
                 })
  return(out)
}

##### iii) Parámetros----

# Día de hoy
HOY <- ymd(today())

# Eliminación de tildes
con_tilde <- c("Á", "É", "Í", "Ó", "Ú")
sin_tilde <- c("A", "E", "I", "O", "U")

# Codificación de RUC y DNI
original <- c("1", "2", "3", "4", "5",
              "6", "7", "8", "9", "0")
codificado <- c("J", "B", "H", "U", "X",
                "F", "S", "3", "T", "W" )



### I.5 Diccionario de variables y otras fuentes ====

##### i) Directorio General y Diccionario de Variables----

# Descarga
dirOPA <- read_sheet(ss = dirOPA_drive, sheet = "DIRECTORIO")
dicOPA <- read_sheet(ss = dirOPA_drive, sheet = "DIC_VARIABLES")

# ii) Diccionario de variables

# Actualizaciones

dir_Loop_Act <- dirOPA %>%
  filter(`TIPO UNIDAD ORGÁNICA`== "OD",
         DATASET == "ODE - 1) REGISTRO DE ACTUALIZACIÓN") %>%
  mutate(Tabla = "ACTUALIZACIONES")

# Descripción Sede Central

dic_RG_SC_D <- dicOPA %>%
  filter(DATASET == "REGISTRO GENERAL DE PROBLEMÁTICAS AMBIENTALES",
         TABLA == "DESCRIPCION",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))

# Descripción ODES (Loop)
dir_Loop_Act_D <- dir_Loop_Act %>%
  select(c(1,5)) %>%
  mutate(Tabla="DESCRIPCION")

# EFA Sede Central
dic_RG_SC_E <- dicOPA %>%
  filter(DATASET == "REGISTRO GENERAL DE PROBLEMÁTICAS AMBIENTALES",
         TABLA == "EFA",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))

# EFA ODES (Loop)
dir_Loop_Act_E <- dir_Loop_Act %>%
  select(c(1,5)) %>%
  mutate(Tabla = "EFA")

# Administrado Sede Central

dic_RG_SC_AD <- dicOPA %>%
  filter(DATASET == "REGISTRO GENERAL DE PROBLEMÁTICAS AMBIENTALES",
         TABLA == "ADMINISTRADO",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))

# Administrado ODES (Loop)

dir_Loop_Act_AD <- dir_Loop_Act %>%
  select(c(1,5)) %>%
  mutate(Tabla = "ADMINISTRADO")

# 4. Cierres 

# Sede Central
dic_RG_SC_CI <- dicOPA %>%
  filter(DATASET == "REGISTRO GENERAL DE PROBLEMÁTICAS AMBIENTALES",
         TABLA == "CIERRES",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))
# Cierres ODES (Loop)
dir_Loop_Act_CI <- dir_Loop_Act %>%
  select(c(1,5)) %>%
  mutate(Tabla = "CIERRES")

# 5. Actualizaciones
# Sede Central 2020
dic_ACT_2020 <- dicOPA %>%
  filter(DATASET == "SC - 1) REGISTRO DE ACTUALIZACIÓN-2020",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))
# Sede Centraal 2021
dic_ACT_2021 <- dicOPA %>%
  filter(DATASET == "SC - 1) REGISTRO DE ACTUALIZACIÓN-2021",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))
# Registros de Actualización ODES
dir_Loop_Act_A <- dir_Loop_Act %>%
  mutate(Tabla = "ACTUALIZACIONES")

# 6. Seguimiento
# Sede Central 2020
dic_SEG_2020 <- dicOPA %>%
  filter(DATASET == "SC - 2) REGISTRO DE SEGUIMIENTO-2020",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))
# Sede Central 2021
dic_SEG_2021 <- dicOPA %>%
  filter(DATASET == "SC - 2) REGISTRO DE SEGUIMIENTO-2021",
         `RELEVANTE PARA LA WEB` == "SI") %>%
  select(c(3,4))
# Seguimiento ODES (Loop)
dir_Loop_Seg <- dirOPA %>%
  filter(`TIPO UNIDAD ORGÁNICA`== "OD",
         DATASET == "ODE - 2) REGISTRO DE SEGUIMIENTO") %>%
  mutate(Tabla = "SEGUIMIENTO")

# 7. Población beneficiada
POB_DIST <- read_sheet(ss = EFA_lista,
                       sheet = "Lista de EFA")





#---------------- II. Descarga y limpieza de datos -------------

### II.1 Descripción de Problemas Ambientales ====

##### i) Sede Central----

# Descarga de informacion
SC_RG_D <- read_sheet(ss = SC_RG_ID, sheet = "DESCRIPCION")
# Uso del diccionario de variables
SC_RG_D_F <- subset(SC_RG_D, select = dic_RG_SC_D$`CAMPO EN BD`)
colnames(SC_RG_D_F) <- dic_RG_SC_D$`CODIGO CAMPO`
# Control de Estado de Problema
table(SC_RG_D_F$ESTADO, 
      useNA = "always")

##### ii) Base de ODES----
# Loop de consolidado
OD_DES <- mapply(R_bajar_apilar_OD,
                 dir_Loop_Act_D$Link, 
                 dir_Loop_Act_D$DATASET,
                 dir_Loop_Act_D$Tabla,
                 SIMPLIFY = FALSE)
# Consolidado
OD_CONSOLIDADO_DES <- do.call(rbind, OD_DES)
# Control de Estado y Origen
OD_CONSOLIDADO_DES <- OD_CONSOLIDADO_DES %>%
  filter(!is.na(COD_PROBLEMA))
# Tabla
table(OD_CONSOLIDADO_DES$ESTADO,
      OD_CONSOLIDADO_DES$ORIGEN,
      useNA = "always")
# Borrar lista de ODES
rm(OD_DES) 



### II.2 Registro de EFAs ====

##### i) Sede Central----
# Descarga de informacion
SC_RG_E <- read_sheet(ss = SC_RG_ID, sheet = "EFA")
# Selección de Variables relevantes
SC_RG_E_F <- subset(SC_RG_E, select = dic_RG_SC_E$`CAMPO EN BD`)
colnames(SC_RG_E_F) <- dic_RG_SC_E$`CODIGO CAMPO`
# Control de registro correcto de EFA
table(SC_RG_E_F$T_EFA, 
      useNA = "always")

##### ii) Base de ODES----
# Loop de consolidado
OD_EFA <- mapply(R_bajar_apilar_OD,
                 dir_Loop_Act_E$Link, 
                 dir_Loop_Act_E$DATASET,
                 dir_Loop_Act_E$Tabla,
                 SIMPLIFY = FALSE)
# Consolidado
OD_CONSOLIDADO_EFA <- do.call(rbind, OD_EFA)
# Control de registro correcto de EFA
OD_CONSOLIDADO_EFA <- OD_CONSOLIDADO_EFA %>%
  filter(!is.na(COD_PROBLEMA))
table(OD_CONSOLIDADO_EFA$T_EFA, 
      OD_CONSOLIDADO_EFA$ORIGEN,
      useNA = "always")




### II.3 Registro de Administrados ====

##### i) Sede Central----
# Descarga de informacion
SC_RG_AD <- read_sheet(ss = SC_RG_ID, sheet = "ADMINISTRADO")
# Selección de Variables relevantes
SC_RG_AD_F <- subset(SC_RG_AD, select = dic_RG_SC_AD$`CAMPO EN BD`)
colnames(SC_RG_AD_F) <- dic_RG_SC_AD$`CODIGO CAMPO`
# Control de registro correcto de administrado
table(SC_RG_AD_F$CATEGORIA,
      useNA = "always")

##### ii) Base de ODES----
# Loop de consolidado
OD_ADM <- mapply(R_bajar_apilar_OD,
                 dir_Loop_Act_AD$Link, 
                 dir_Loop_Act_AD$DATASET,
                 dir_Loop_Act_AD$Tabla,
                 SIMPLIFY = FALSE)
# Consolidado
OD_CONSOLIDADO_ADM <- do.call(rbind, OD_ADM)
# Control de registro correcto de administrado
table(OD_CONSOLIDADO_ADM$CATEGORIA,
      OD_CONSOLIDADO_ADM$ORIGEN,
      useNA = "always")
# ELiminar lista
rm(OD_ADM)




### II.4 Registro de Cierres ====

##### i) Sede Central----
# Descarga de informacion
SC_RG_CI <- read_sheet(ss = SC_RG_ID, sheet = "CIERRES")
# Selección de Variables relevantes
SC_RG_CI_F <- subset(SC_RG_CI, select = dic_RG_SC_CI$`CAMPO EN BD`)
colnames(SC_RG_CI_F) <- dic_RG_SC_CI$`CODIGO CAMPO`
# Control de registro correcto de fechas
table(SC_RG_CI_F$T_CIERRE,
      useNA = "always")

##### ii) Base de ODES----
# Loop de consolidado
OD_CIE <- mapply(R_bajar_apilar_OD,
                 dir_Loop_Act_CI$Link, 
                 dir_Loop_Act_CI$DATASET,
                 dir_Loop_Act_CI$Tabla,
                 SIMPLIFY = FALSE)
# Consolidado
OD_CONSOLIDADO_CI <- do.call(rbind, OD_CIE)
# Control de registro correcto de fechas
OD_CONSOLIDADO_CI <- OD_CONSOLIDADO_CI %>%
  filter(!is.na(COD_PROBLEMA))
table(OD_CONSOLIDADO_CI$T_CIERRE, 
      useNA = "always")
# Eliminar la lista
rm(OD_CIE)



### II.5 Registro de Actualizaciones ====

##### i) Base 2020----
# Descarga de informacion
SC_A_2020 <- read_sheet(ss = ACT_2020, sheet = "ACTUALIZACIONES")
# Selección de Variables relevantes
SC_ACT_2020 <- subset(SC_A_2020, select = dic_ACT_2020$`CAMPO EN BD`)
colnames(SC_ACT_2020) <- dic_ACT_2020$`CODIGO CAMPO`

##### ii) Base 2021----
# Descarga de informacion
SC_A_2021 <- read_sheet(ss = ACT_2021, sheet = "ACTUALIZACIONES")
# Selección de Variables relevantes
SC_ACT_2021 <- subset(SC_A_2021, select = dic_ACT_2021$`CAMPO EN BD`)
colnames(SC_ACT_2021) <- dic_ACT_2021$`CODIGO CAMPO`

##### iii) Consolidado de Sede Central 2020 y 2021----
SC_CONSOLIDADO_ACT <- rbind(SC_ACT_2021, SC_ACT_2020)

##### iv) Base de ODES----
# Descarga de información y selección de info relevante
OD_ACTUAL <- mapply(R_bajar_apilar_OD,
                    dir_Loop_Act_A$Link, 
                    dir_Loop_Act_A$DATASET, 
                    dir_Loop_Act_A$Tabla, 
                    SIMPLIFY = FALSE)
# Consolidado
OD_CONSOLIDADO_ACT <- do.call(rbind, OD_ACTUAL)
# Eliminar la lista
rm(OD_ACTUAL)



### II.6 Registro de Seguimiento ====

##### i) Base 2020----
# Descarga de informacion
SC_S_2020 <- read_sheet(ss = SEG_2020, sheet = "SEGUIMIENTO")
# Selección de Variables relevantes
SC_SEG_2020 <- subset(SC_S_2020, select = dic_SEG_2020$`CAMPO EN BD`)
colnames(SC_SEG_2020) <- dic_SEG_2020$`CODIGO CAMPO`
# Formato de fecha
SC_SEG_2020 <- mutate(SC_SEG_2020, F_NOT_OCI = ymd(F_NOT_OCI)) 
# Control de correcto registro de EFA
table(SC_SEG_2020$T_EFA,
      SC_SEG_2020$TAREA,
      useNA = "always")

##### ii) Base 2021----
# Descarga de informacion
SC_S_2021 <- read_sheet(ss = SEG_2021, sheet = "SEGUIMIENTO",
                        skip = 2)
# Selección de variables relevantes
SC_SEG_2021 <- subset(SC_S_2021, select = dic_SEG_2021$`CAMPO EN BD`)
colnames(SC_SEG_2021) <- dic_SEG_2021$`CODIGO CAMPO`
# Formato fecha
SC_SEG_2021 <- mutate(SC_SEG_2021, F_NOT_OCI = ymd(F_NOT_OCI))
# Control de correcto registro de EFA
table(SC_SEG_2021$T_EFA,
      SC_SEG_2021$TAREA,
      useNA = "always")

##### iii) Consolidado de Sede Central 2020 y 2021----
SC_CONSOLIDADO_S <- rbind(SC_SEG_2021, SC_SEG_2020)


##### iv) Base de ODES----
# Descarga de información y selección de info relevante
OD_SEG_L <- mapply(R_bajar_apilar_OD,
                   dir_Loop_Seg$Link, 
                   dir_Loop_Seg$DATASET,
                   dir_Loop_Seg$Tabla,
                   SIMPLIFY = FALSE)
# Consolidado de ODES
OD_CONSOLIDADO_S <- do.call(rbind, OD_SEG_L)
# Conteo  de NA (EFA registradas incorrectamente)
OD_CONSOLIDADO_S <- OD_CONSOLIDADO_S %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(EFA))
# Control de correcto registro de EFA
table(OD_CONSOLIDADO_S$T_EFA,
      useNA = "always")
# Eliminar lista
rm(OD_SEG_L)



#-------- III. Procesamiento y tratamiento de datos #-------

### III.1 Descripción de Problemas Ambientales ====

##### i) Variables Auxiliares----
# Sede Central
SC_RG_D_F <- SC_RG_D_F %>%
  mutate(ORIGEN="SEFA") %>%
  filter(!is.na(COD_PROBLEMA) |
         !is.na(ESTADO)) %>%
  # Estado auxiliar
  mutate(ESTADO_AUX = case_when(ESTADO == "CERRADO" ~ "Problema ambiental solucionado",
                                TRUE ~ "Problema ambiental en seguimiento"))
# Control de Estado
table(SC_RG_D_F$ESTADO,
      SC_RG_D_F$ESTADO_AUX)
# Control de unicidad
length(unique(SC_RG_D_F$COD_PROBLEMA)) == length(SC_RG_D_F$COD_PROBLEMA)

# ODES
OD_CONSOLIDADO_DES_F <- OD_CONSOLIDADO_DES %>%
  filter(!is.na(COD_PROBLEMA),
         ORIGEN == "ODE") %>%
  # Estado auxiliar
  mutate(ESTADO_AUX = case_when(ESTADO == "CERRADO" ~ "Problema ambiental solucionado",
                              TRUE ~ "Problema ambiental en seguimiento"))
# Control de Estado
table(OD_CONSOLIDADO_DES_F$ESTADO,
      OD_CONSOLIDADO_DES_F$ESTADO_AUX)
# Control de unicidad
length(unique(OD_CONSOLIDADO_DES_F$COD_PROBLEMA)) == length(OD_CONSOLIDADO_DES_F$COD_PROBLEMA)

##### ii) Consolidado de Sede Central y ODES----
DESCRIPCION <- rbind(OD_CONSOLIDADO_DES_F, SC_RG_D_F)



### III.2 Registro de EFAs ====

##### i) Variables auxiliares----
# ODES
OD_CONSOLIDADO_EFA_F <- OD_CONSOLIDADO_EFA %>%
  mutate(ORIGEN = "ODE") %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(T_EFA),
         !is.na(ALCANCE),
         !is.na(B_LEGAL))

# Sede Central
SC_CONSOLIDADO_EFA_F <- SC_RG_E_F %>%
  mutate(ORIGEN="SEFA") %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(T_EFA),
         !is.na(ALCANCE),
         !is.na(B_LEGAL))

# iii) Consolidado de Sede Central y ODES
EFA <- rbind(OD_CONSOLIDADO_EFA_F, SC_CONSOLIDADO_EFA_F)



### III.3 Registro de Administrado ====

##### i) Variables auxiliares----
# ODES
OD_CONSOLIDADO_ADM_F <- OD_CONSOLIDADO_ADM %>%
  mutate(ORIGEN = "ODE",
         DNI_RUC = as.character(DNI_RUC)) %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(CATEGORIA),
         !is.na(DNI_RUC),
         CATEGORIA != "NO ESPECIFICA",
         DNI_RUC != "NO APLICA",  DNI_RUC != "NULL")

# Sede Central
SC_CONSOLIDADO_ADM_F <- SC_RG_AD_F %>%
  mutate(ORIGEN="SEFA",
         DNI_RUC = as.character(DNI_RUC))  %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(CATEGORIA),
         !is.na(DNI_RUC),
         CATEGORIA != "NO ESPECIFICA",
         DNI_RUC != "NO APLICA",  DNI_RUC != "NULL")

# iii) Consolidado de Sede Central y ODES
ADMINISTRADO_ORIGINAL <- rbind(OD_CONSOLIDADO_ADM_F, SC_CONSOLIDADO_ADM_F)
# Codificación de DNI y RUC
ADMINISTRADO <- ADMINISTRADO_ORIGINAL %>%
  mutate(DNI_RUC = stri_replace_all_regex(DNI_RUC, original, 
                                          codificado, vectorize = F))
  
 


### III.4 Registro de Cierre ====

##### i) Variables auxiliares----
# ODES
OD_CONSOLIDADO_CI_F <- OD_CONSOLIDADO_CI %>%
  mutate(ORIGEN = "ODE") %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(FECHA))

# Sede Central
SC_RG_CI_F <- SC_RG_CI_F %>%
  mutate(ORIGEN="SEFA") %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(FECHA))

##### ii) Consolidado de Sede Central y ODES----
CIERRES <- rbind(OD_CONSOLIDADO_CI_F, SC_RG_CI_F)
# Cambio de fecha
CIERRES <- CIERRES %>%
  mutate(FECHA = ymd(FECHA)) %>%
  filter(!is.na(COD_PROBLEMA))



### III.5 Registro de Actualizaciones ====

##### i) Variables Auxiliares----
# Sede Central
SC_CONSOLIDADO_ACT_F <- SC_CONSOLIDADO_ACT %>%
  mutate(ORIGEN = "SEFA") %>%
  filter(!is.na(COD_PROBLEMA))
# Transformación ODES Temporal
OD_CONSOLIDADO_ACT_F <- OD_CONSOLIDADO_ACT %>%
  mutate(ORIGEN = "ODE") %>%
  filter(!is.na(COD_PROBLEMA))

##### ii) Consolidado de Sede Central y ODES----
ACTUALIZACIONES <- rbind(SC_CONSOLIDADO_ACT_F, OD_CONSOLIDADO_ACT_F)



### III.6 Registro de Seguimiento ====

##### i) Variables Auxiliares----
# Sede Central
SC_CONSOLIDADO_S_F <- SC_CONSOLIDADO_S %>%
  filter(!is.na(TAREA)) %>%
  mutate(ORIGEN = "SEFA")

# ODES
OD_CONSOLIDADO_S_F <- OD_CONSOLIDADO_S %>%
  mutate(ORIGEN = "ODE",
         TAREA = "Pedido de información")

##### ii) Consolidado de Sede Central y ODES----
SEGUIMIENTO_SIN_DIAS <- rbind(SC_CONSOLIDADO_S_F, 
                              OD_CONSOLIDADO_S_F)
# Conteo de NA
table(SEGUIMIENTO_SIN_DIAS$TAREA, 
      SEGUIMIENTO_SIN_DIAS$T_EFA, 
      useNA = "always")

# Estimación de días hábiles
SEGUIMIENTO_CON_DIAS <- SEGUIMIENTO_SIN_DIAS %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(EFA))  %>%
  # Auxiliar de respuesta
  mutate(F_NOT = ymd(F_NOT),
         F_NOT_REIT = ymd(F_NOT_REIT),
         F_NOT_OCI = ymd(F_NOT_OCI),
         F_RPTA = ymd(F_RPTA),
         F_MAX_RPTA = ymd(F_MAX_RPTA),
         F_CORTE = HOY,
         PERIODO_NOT = year(F_NOT),
         PERIODO_RPTA = year(F_RPTA),
         N_RPTA = case_when(is.na(F_RPTA) ~ 0,
                            TRUE ~ 1),
         T_RPTA = case_when(is.na(F_RPTA) ~ bizdays(F_NOT, F_CORTE, cal_peru),
                            TRUE ~ bizdays(F_NOT, F_RPTA, cal_peru)),
         TIPO_RPTA = case_when(!is.na(F_RPTA) & F_RPTA <= F_MAX_RPTA ~ "Atendido en plazo",
                               !is.na(F_RPTA) & F_RPTA > F_MAX_RPTA ~ "Atendido fuera de plazo",
                               is.na(F_RPTA) & F_MAX_RPTA >= HOY ~ "En plazo",
                               is.na(F_RPTA) & F_MAX_RPTA < HOY ~ "Vencido",
                               TRUE ~ ""))

# Definición de Abreviaturas
EFA_ABREV <- read_sheet(ss = EFA_dir,
                        sheet = "Directorio")
# Abreviatura para el match
EFA_ABREV_F <- EFA_ABREV %>%
  mutate(EFA = `Entidad u oficina`,
         T_EFA = `Tipo de entidad u oficina`,
         EFA_ABREVIADO = `EFA ABREVIADO`) %>%
  select(c(EFA, T_EFA,
           EFA_ABREVIADO))

# Match de abreviatura
SEGUIMIENTO <- merge(SEGUIMIENTO_CON_DIAS, 
                     EFA_ABREV_F)


### III.7 Población relacionada al problema ambiental ====

##### i) Obtención de ID Distrital----
ACTUALIZACIONES_ID_POB <- ACTUALIZACIONES %>%
  select(COD_PROBLEMA,
         DPTO, PROVINCIA, DISTRITO) %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(DPTO),
         !is.na(PROVINCIA),
         !is.na(DISTRITO)) %>%
  # Eliminación de tildes y creación de ID_MUNI
  mutate(DPTO = gsub("CUZCO", "CUSCO", DPTO),
         DPTO = stri_replace_all_regex(DPTO, con_tilde, sin_tilde, vectorize = F),
         PROVINCIA = stri_replace_all_regex(PROVINCIA, con_tilde, sin_tilde, vectorize = F),
         DISTRITO = stri_replace_all_regex(DISTRITO, con_tilde, sin_tilde, vectorize = F),
         ID_MUNI = paste(DPTO, PROVINCIA, DISTRITO,
                           "DISTRITAL",
                           sep="-"))

##### ii) Tabla de población----
POBLACION <- POB_DIST %>%
  filter(NIVEL == "GOBIERNO DISTRITAL") %>%
  mutate(ID_MUNI = `ID MUNI`,
         P_DISTRITAL = `POBLACION (PROYECCIÓN AL 2019)`) %>%
  select(ID_MUNI, P_DISTRITAL) 

# Merge
ACTUAL_POB <- merge(ACTUALIZACIONES_ID_POB,
                    POBLACION,
                    all.x = T)

##### iii) Resumen por Distrito----
POB_POR_PROBLEMA_DIST <- ACTUAL_POB %>%
  filter(!is.na(P_DISTRITAL)) %>%
  group_by(COD_PROBLEMA, 
           ID_MUNI,
           DPTO, PROVINCIA, DISTRITO, 
           P_DISTRITAL)  %>%
  summarise() %>%
  arrange(-P_DISTRITAL)  %>%
  ungroup()
# Número de problemas sin ubicación correcta
nrow(DESCRIPCION) - length(unique(POB_POR_PROBLEMA_DIST$COD_PROBLEMA))

##### iv) Resumen por Departamento----

POB_POR_PROBLEMA_DPTO <- POB_POR_PROBLEMA_DIST %>%
  # Solo los distritos únicos
  distinct(DPTO, P_DISTRITAL) %>%
  filter(!is.na(P_DISTRITAL)) %>%
  # Suma por departamento
  group_by(DPTO)  %>%
  summarise(P_DEPARTAMENTO = sum(P_DISTRITAL)) %>%
  arrange(-P_DEPARTAMENTO)
# Control de población departamental y distrital
sum(POB_POR_PROBLEMA_DPTO$P_DEPARTAMENTO)

##### v) Consolidado----
POB_POR_PROBLEMA <- merge(POB_POR_PROBLEMA_DIST,
                          POB_POR_PROBLEMA_DPTO,
                          all.x = TRUE)
# Conteo de NA
sum(is.na(POB_POR_PROBLEMA$P_DISTRITAL))
sum(is.na(POB_POR_PROBLEMA$P_DEPARTAMENTO))
# Eliminación de variables
POB_POR_PROBLEMA_F <- POB_POR_PROBLEMA %>%
  select(-c(DPTO))



### III.8 Descripción actualizada de problemas ambientales ====

##### i) Actualizaciones con población relacionada----
ACTUALIZACIONES_POB <- merge(ACTUALIZACIONES,
                             POB_POR_PROBLEMA_F,
                             all.x = TRUE)
##### ii) Conteo de NA por origen----
table(ACTUALIZACIONES_POB$ORIGEN, 
      is.na(ACTUALIZACIONES_POB$ID_MUNI))

##### iii) Descripción de problemas actualizado----
DES_ACT_POB <- merge(DESCRIPCION,
                      ACTUALIZACIONES_POB,
                      all.x = TRUE)
# Control de problemas sin departamento
DES_ACT_SIN_DPTO <- DES_ACT_POB %>%
  select(-c(DPTO))
##### iv) Departamento de problema, según actualizaciones----
COD_DPTO <- ACTUALIZACIONES %>%
  distinct(COD_PROBLEMA, DPTO)  %>%
  filter(!is.na(COD_PROBLEMA),
         !is.na(DPTO))
##### v) Cantidad  de problemas con Departamento----
length(unique(COD_DPTO$COD_PROBLEMA))
##### vi) Merge para identificar departamento----
DES_PROBLEMA <- merge(DES_ACT_SIN_DPTO,
                      COD_DPTO,
                      all.x = T)
# Control de problemas sin departamento (no ubicables en mapa)
table(DES_PROBLEMA$DPTO, useNA = "always")

# Control de ID igual al número de problemas en descripción
nrow(DESCRIPCION) - length(unique(DES_PROBLEMA$COD_PROBLEMA))



### III.9 Administrado y EFA involucrado ====

##### i) Efa y administrados involucrados, por problema----
EFA_ADMINISTRADO <- merge(EFA,
                          ADMINISTRADO,
                          all.x = TRUE, 
                          all.y = TRUE)

##### ii) Problema, descripción y departamento----
PROB_DES_DPTO <- DES_PROBLEMA %>%
  distinct(COD_PROBLEMA, ESTADO, ESTADO_AUX, DPTO, DES_PROBLEMA)

##### iii) Consolidado para filtro general----
DES_EFA_ADM <- merge(EFA_ADMINISTRADO,
                     PROB_DES_DPTO)


### III.10 Seguimiento y descripción de problemas ambientales ====

##### i) Región del seguimiento----
SEGUIMIENTO_CON_DPTO <- merge(SEGUIMIENTO,
                              PROB_DES_DPTO)

##### ii) Orden de matriz----
SEG_PROBLEMA <- SEGUIMIENTO_CON_DPTO %>%
  filter(!is.na(HT)) %>%
  mutate(ESTADO_AUX = case_when(ESTADO == "CERRADO" ~ "Problema ambiental solucionado",
                                TRUE ~ "Problema ambiental en seguimiento"))  %>%
  relocate(ORIGEN, COD_PROBLEMA,
           ESTADO_AUX, DPTO,
           TAREA, REQ_DETALLE, DES_PROBLEMA,
           EFA, T_EFA, EFA_ABREVIADO,
           DOC_ENVIADO, F_NOT,
           DOC_REIT, F_NOT_REIT,
           DOC_OCI, F_NOT_OCI,
           RPTA, F_RPTA, T_RPTA)
##### iii) Conteo NA----
sum(is.na(SEG_PROBLEMA$DPTO))



#----- IV. Subida de información para publicación #-------

### IV.1 Descripción de problemas ====
write_sheet(DESCRIPCION,
            drive_consolidado,
            "DESCRIPCION")

### IV.2 EFAs involucradas ====
write_sheet(EFA,
            drive_consolidado,
            "EFA")

### IV.3 Administrados involucrados ====
write_sheet(ADMINISTRADO,
            drive_consolidado,
            "ADMINISTRADO")


### IV.4 Registro de Cierres ====
write_sheet(CIERRES,
            drive_consolidado,
            "CIERRES")

### IV.5 Registro de Actualizaciones ====
write_sheet(ACTUALIZACIONES,
            drive_consolidado,
            "ACTUALIZACIONES")

### IV.6 Registro de Seguimiento ====
write_sheet(SEGUIMIENTO, 
            drive_consolidado,
            "SEGUIMIENTO")

### IV.7 Problemas Ambientales Actualizados ====
write_sheet(DES_PROBLEMA, 
            drive_consolidado,
            "DES_PROBLEMA")

### IV.8 EFA y Administrado involucradas ====
write_sheet(DES_EFA_ADM, 
            drive_consolidado,
            "DES_EFA_ADM")

### IV.9 Problemas Ambientales en Seguimiento ====
write_sheet(SEG_PROBLEMA, 
            drive_consolidado,
            "SEG_PROBLEMA")
