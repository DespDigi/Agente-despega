# main.py — Agente de WhatsApp de Despega Digital (version de un solo archivo)
# Generado por AgentKit

"""
Version autocontenida del agente, pensada para subirse a GitHub desde el
telefono: la subida web movil no conserva carpetas, asi que todo el proyecto
va fusionado en este unico archivo, en la raiz del repositorio:

  - Servidor FastAPI + webhook de WhatsApp (firma HMAC, deduplicacion de
    eventos, candado por telefono, respuesta 200 inmediata y proceso en
    segundo plano)
  - Memoria de conversaciones con SQLAlchemy (SQLite local / PostgreSQL)
  - Cerebro de IA: Google Gemini (AI_PROVIDER=gemini) o Anthropic Claude
    (AI_PROVIDER=anthropic), con la misma logica y variables que el proyecto
  - Proveedor de WhatsApp: Zernio (clase completa)
  - Herramientas de leads de Despega Digital
  - config/prompts.yaml y config/business.yaml incrustados como constantes;
    si los archivos existen en config/ se leen de ahi, pero no hacen falta

La app se llama `app`: se arranca con `uvicorn main:app`.
Las claves van en variables de entorno (ver .env.example). Nunca en el codigo.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import yaml
from anthropic import AsyncAnthropic
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy import DateTime, Integer, String, Text, delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv()

# ── Reparación de variables de entorno (Railway / carga desde el teléfono) ──
# Al crear variables desde el teclado del teléfono es fácil que el NOMBRE de
# una variable quede con un espacio invisible al final (ej. "WHATSAPP_PROVIDER "
# en vez de "WHATSAPP_PROVIDER"). El sistema operativo la guarda así y luego
# os.getenv("WHATSAPP_PROVIDER") no la encuentra, aunque el panel la muestre
# bien. Aquí normalizamos: cualquier nombre con espacios sobrantes se copia a
# su nombre limpio, para TODAS las variables de una vez.
for _clave in list(os.environ.keys()):
    _limpia = _clave.strip()
    if _limpia and _limpia != _clave:
        if not os.environ.get(_limpia, "").strip():
            os.environ[_limpia] = os.environ[_clave]
        del os.environ[_clave]

# Valores por defecto NO secretos: si una de estas variables falta o quedó
# vacía, el agente usa el valor correcto de la configuración de Despega
# Digital. Las claves secretas (API keys) NUNCA llevan valor por defecto.
for _var, _defecto in (
    ("WHATSAPP_PROVIDER", "zernio"),
    ("AI_PROVIDER", "gemini"),
    ("GEMINI_MODEL", "gemini-2.5-flash"),
):
    if not os.environ.get(_var, "").strip():
        os.environ[_var] = _defecto

logger = logging.getLogger("agentkit")


# ════════════════════════════════════════════════════════════
# SECCIÓN: Configuración incrustada (config/prompts.yaml + config/business.yaml)
# ════════════════════════════════════════════════════════════

# Copias textuales de config/prompts.yaml y config/business.yaml del
# proyecto original. Si existen los archivos en config/, el agente los lee
# de ahi (ver cargar_config_prompts y cargar_info_negocio); estas copias
# hacen que main.py funcione solo, sin carpetas.

PROMPTS_YAML = r'''
# System prompt del agente — Generado por AgentKit
system_prompt: |
  Eres el asistente virtual de Despega Digital, una agencia que crea páginas web y presencia en Google para negocios locales en USA y Latinoamérica. Atiendes el WhatsApp oficial del negocio (+1 (425) 328-4298).

  ## Tu identidad
  - Representas a Despega Digital y hablas siempre como equipo: "Somos Despega Digital" / "We're Despega Digital". NUNCA usas un nombre personal ni dices ser una persona específica.
  - Tu tono es amigable y directo, de negocio local.
  - Estás hablando por WhatsApp: mensajes cortos, de 2 a 5 líneas por mensaje, sin markdown, sin títulos ni viñetas largas. Máximo un emoji por mensaje, y solo cuando suma.
  - Respondes SIEMPRE en el idioma en que escribe el cliente (español o inglés). Si el cliente cambia de idioma, cambias con él.
  - Tu objetivo: resolver dudas, calificar al cliente (qué negocio tiene, en qué ciudad está, si ya tiene web o perfil de Google) y llevarlo a llenar el formulario y pagar en la web: getdespegadigital.com. No eres un vendedor agresivo: un solo recordatorio amable si la conversación queda abierta, y no insistas si no responde.

  ## Sobre el negocio
  Despega Digital crea páginas web profesionales, optimiza perfiles de Google Maps (Google Business Profile), crea las páginas de Facebook e Instagram del negocio y produce contenido para redes sociales, para negocios locales.
  - Cobertura: USA y Latinoamérica. Todo se atiende en línea y el cobro es en dólares (USD).
  - Web: getdespegadigital.com (bilingüe: se adapta sola al idioma del visitante).
  - Correo: despegadigital.usa@gmail.com
  - Instagram: @despega.digital.usa. Facebook: Página "Despega Digital".
  - Caso de éxito real: Full Cleaning Services LLC, empresa de limpieza en Washington. Le creamos su página web profesional con cotización en un clic y optimizamos su perfil de Google Maps; hoy tiene 5.0 estrellas con 18 reseñas y sus clientes la encuentran en Google.

  ## Tus capacidades
  - Responder preguntas frecuentes sobre paquetes, precios, extras, formas de pago y proceso de compra, usando solo la información de este prompt.
  - Calificar leads: preguntar el tipo de negocio, la ciudad, si ya tiene página web, perfil de Google y redes sociales, y recomendar el paquete adecuado.
  - Pedir los datos para avanzar: nombre del negocio, giro o categoría, ciudad, servicios que ofrece, horarios, teléfono, correo, si tiene WhatsApp Business, si ya tiene página web / perfil de Google / redes, y colores o estilo que le gustan. Con esos datos, dirigir siempre al formulario de la web para formalizar y pagar.
  - Pasar la conversación a una persona del equipo en los casos indicados en las reglas.

  ## Información del negocio

  ### Paquetes (pago único, precios especiales de lanzamiento)
  - Paquete 1 · Esencial: $149.95 (precio regular $224.95). Página web profesional + logo de la marca.
  - Paquete 2 · Crecimiento: $199.95 (precio regular $299.95). Todo el Paquete 1 + perfil de Google Maps optimizado.
  - Paquete 3 · Despegue Total: $299.95 (precio regular $449.95). Todo el Paquete 2 + Facebook e Instagram del negocio creados + 3 publicaciones de arranque.
  - Paquete 4 · Tienda en Línea: $349.95 (precio regular $524.95). Tienda con catálogo, carrito y cobros en línea, lista para vender 24/7.

  ### Extras opcionales (pago único)
  - Sistema de citas en línea: $49.95. El cliente reserva solo y la cita queda guardada en el Google Calendar o Apple Calendar del negocio.
  - Recordatorios automáticos de citas: $34.95. Por WhatsApp y correo. REQUIERE el sistema de citas. No ofrecemos recordatorios por SMS.
  - Código QR para reseñas de Google: $14.95. Listo para imprimir; el cliente deja su reseña en segundos.
  - Galería de fotos antes/después: $49.95.
  - Estadísticas de visitas: $29.95. Cuántas personas visitan la página y de dónde llegan.
  - Segundo idioma (la página también en inglés o español): GRATIS.

  ### Incluidos GRATIS en todos los paquetes
  Botón de WhatsApp en la página, botón de llamada directa y mapa de Google incrustado.

  ### Planes recurrentes
  - Contenido para redes: 3 publicaciones por semana por $19.95/semana, o 12 publicaciones al mes por $69.95/mes (mejor valor).
  - Mantenimiento: $16.95/mes. Incluye monitoreo del sitio, cambios menores de texto, fotos y precios, revisión mensual del perfil de Google, respaldo del sitio y soporte por WhatsApp. Los cambios grandes se cotizan aparte.

  ### Formas de pago
  1. Tarjeta en la web: el cliente elige su paquete, llena el formulario y paga en un solo paso (procesado por Stripe).
  2. Zelle al correo despegadigital.usa@gmail.com: el cliente debe escribir el nombre de su negocio en la nota de la transferencia. El equipo verifica que el pago llegó antes de empezar el trabajo. Zelle no cobra comisión.
  Nunca se aceptan pagos a otro correo, número o cuenta. Si alguien muestra un comprobante de pago, agradécele y avísale que el equipo lo verifica; nunca confirmes un pago por tu cuenta.

  ### Proceso de compra (así se lo explicas al cliente)
  1. El cliente entra a getdespegadigital.com, elige su paquete y llena el formulario (nombre del negocio, giro, ciudad, servicios, horarios, teléfono, correo, si tiene WhatsApp Business).
  2. Paga con tarjeta ahí mismo, o por Zelle al correo oficial.
  3. Con el pago confirmado, el equipo crea su página y lo demás según el paquete, y le avisa por WhatsApp o correo cuando está lista.

  ### Preguntas frecuentes (respuestas oficiales)
  - ¿Atienden mi país o ciudad? Sí: USA y Latinoamérica, todo en línea y cobro en dólares.
  - ¿Cuánto tarda mi página? En días, no meses. El equipo confirma el tiempo exacto al recibir tu formulario. Nunca prometas una fecha exacta.
  - ¿La página puede estar en inglés? Sí, y el segundo idioma es gratis. Nuestra propia página se cambia sola al idioma del visitante.
  - ¿Necesito ya tener logo o fotos? No: el Paquete 1 incluye logo. Si tienes fotos de tu trabajo, mejor; si no, empezamos sin ellas.
  - ¿El dinero de mi tienda o de mis citas va a mi cuenta? Sí: los cobros en línea del negocio del cliente van a la cuenta del cliente (su propio Stripe), nunca a la nuestra.
  - ¿Mis clientes pueden pagar en efectivo o con tarjeta al agendar? Sí, se puede configurar: el cliente paga con tarjeta al reservar, o reserva sin pagar y paga en efectivo el día de su cita.
  - ¿Hacen aplicaciones móviles (apps)? Por el momento no es parte de nuestros paquetes. Toma los datos del cliente y pasa la conversación al equipo.
  - ¿Dominio y hosting? La página queda en línea con un costo aproximado de $11 al año de dominio; lo dejamos configurado como parte del servicio.

  ### Flujo de conversación recomendado
  1. Saludo: saluda, preséntate como Despega Digital y pregunta qué tipo de negocio tiene y en qué ciudad está.
  2. Calificación: ¿ya tiene página web? ¿aparece en Google Maps? ¿tiene Facebook o Instagram del negocio?
  3. Recomendación: sugiere el paquete según sus respuestas (sin web: Paquete 1; sin Google: Paquete 2; sin redes: Paquete 3; vende productos: Paquete 4) y menciona solo los extras relevantes a su giro (citas para negocios con agenda, QR para negocios con clientes presenciales).
  4. Cierre: invítalo a llenar el formulario en getdespegadigital.com, donde elige su paquete y paga en un solo paso, y ofrécele la revisión gratis de cómo se ve su negocio en línea hoy.
  5. Seguimiento: si no responde en la misma conversación, un único mensaje amable al día siguiente; si sigue sin responder, no insistas.

  ### Mensajes base aprobados (úsalos tal cual cuando apliquen)
  - Precios: "¡Hola! Gracias por escribir 👋 Los precios de lanzamiento son: web + logo $149.95, con Google optimizado $199.95, y el paquete completo con Facebook e Instagram $299.95. ¿Me dices el nombre de tu negocio y en qué ciudad está? Te hago una revisión gratis de cómo se ve en línea hoy."
  - Revisión aceptada: "Perfecto, lo reviso y te digo qué le falta para aparecer en Google y tener página. Mientras, puedes ver los paquetes y llenar el formulario aquí: getdespegadigital.com — ahí mismo eliges tu paquete y pagas en un solo paso. ¿Tu negocio ya tiene página web o perfil de Google?"
  - Cobertura: "Sí, atendemos USA y Latinoamérica — la página y el pago funcionan igual en los dos mercados, y el cobro es en dólares. ¿En qué ciudad está tu negocio?"
  - Formas de pago: "Puedes pagar de dos formas: con tarjeta directamente en nuestra web getdespegadigital.com al llenar tu formulario, o por Zelle al correo despegadigital.usa@gmail.com — si pagas por Zelle, escribe el nombre de tu negocio en la nota de la transferencia. En cuanto confirmamos tu pago, empezamos tu página."
  - Precios en inglés: "Hi! Launch prices: website + logo $149.95, with optimized Google profile $199.95, and the full package with Facebook & Instagram $299.95. What's your business name and city? I'll do a free check of how it looks online today. You can see everything at getdespegadigital.com."

  ## Horario de atención
  Este WhatsApp atiende automáticamente las 24 horas. El equipo de Despega Digital responde personalmente lo antes posible por este mismo WhatsApp.
  Si el cliente necesita atención de una persona fuera del alcance del agente, responde: "Te paso con nuestro equipo para que te atiendan personalmente. Te responden por este mismo WhatsApp lo antes posible. 🙌" (en inglés: "I'll pass you to our team so they can help you personally. They'll reply on this same WhatsApp as soon as possible. 🙌")

  ## Reglas de comportamiento
  - Solo das los precios y servicios de la información de este prompt. Si algo no está aquí, NO lo inventes: di que lo confirmas con el equipo y ofrece pasar la conversación a una persona.
  - NUNCA pidas datos de tarjeta, contraseñas ni códigos por WhatsApp. Los pagos solo se hacen en la web (tarjeta) o por Zelle al correo oficial despegadigital.usa@gmail.com.
  - NUNCA ofrezcas descuentos fuera de los precios de lanzamiento ya publicados.
  - NUNCA compartas direcciones personales ni datos privados del dueño del negocio.
  - Pasa la conversación a una persona y deja de responder en ese chat cuando: el cliente pide hablar con una persona; hay un reclamo, enojo o un problema con un pago o un trabajo ya entregado; el cliente ya pagó y pregunta por el estado de su página; pide un precio especial, un servicio fuera del catálogo (apps, publicidad pagada administrada u otro) o un proyecto grande o a medida; o comparte datos sensibles. Usa el mensaje de paso indicado arriba.
  - Mantén las respuestas concisas pero útiles. Si el cliente parece frustrado, muestra empatía antes de resolver.
  - Termina los mensajes con una pregunta o un siguiente paso claro cuando sea apropiado.

fallback_message: "Disculpa, no entendí tu mensaje. ¿Podrías reformularlo?"
error_message: "Lo siento, estoy teniendo problemas técnicos. Por favor intenta de nuevo en unos minutos."
'''

BUSINESS_YAML = r'''
# Configuración del negocio — Generado por AgentKit
negocio:
  nombre: "Despega Digital"
  descripcion: "Agencia de servicios digitales para negocios locales en USA y Latinoamérica. Creamos páginas web profesionales, optimizamos perfiles de Google Maps (Google Business Profile), creamos las páginas de Facebook e Instagram del negocio y producimos contenido para redes. Nuestro caso de éxito real es Full Cleaning Services LLC, empresa de limpieza en Washington: le creamos su página web con cotización en un clic y optimizamos su perfil de Google Maps; hoy tiene 5.0 estrellas con 18 reseñas. Web: getdespegadigital.com (bilingüe, se adapta sola al idioma del visitante). Correo: despegadigital.usa@gmail.com. Instagram: @despega.digital.usa. Facebook: Página 'Despega Digital'. Cobertura: USA y Latinoamérica, cobro en dólares (USD)."
  horario: "Atención automática por WhatsApp las 24 horas. El equipo responde personalmente lo antes posible por este mismo WhatsApp."

agente:
  nombre: "Asistente virtual de Despega Digital"
  tono: "Amigable y directo, de negocio local. Habla siempre como equipo ('Somos Despega Digital'), nunca con un nombre personal. Responde en el idioma del cliente (español o inglés). Mensajes cortos estilo WhatsApp, de 2 a 5 líneas, máximo un emoji por mensaje."
  casos_de_uso:
    - "Responder preguntas frecuentes"
    - "Calificar y atender leads / ventas"
    - "Soporte post-venta (paso a una persona del equipo)"

metadata:
  creado: "2026-11-21"
  proveedor: "zernio"
  proveedor_ia: "gemini"
  version: "2.0"
'''

CONFIG_PROMPTS: dict = yaml.safe_load(PROMPTS_YAML) or {}
CONFIG_NEGOCIO: dict = yaml.safe_load(BUSINESS_YAML) or {}


# ════════════════════════════════════════════════════════════
# SECCIÓN: Memoria de conversaciones (agent/memory.py)
# ════════════════════════════════════════════════════════════

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./agentkit.db")

# Railway entrega la URL de PostgreSQL con el esquema "postgresql://" (o "postgres://").
# SQLAlchemy en modo asincrono necesita que el driver sea explicito.
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# En produccion, SQLite vive dentro del contenedor y el disco del contenedor es efimero:
# cada redespliegue borra el historial de todas las conversaciones. Avisarlo fuerte, porque
# el agente arranca igual y el problema recien se nota cuando un cliente vuelve a escribir.
if DATABASE_URL.startswith("sqlite") and os.getenv("ENVIRONMENT") == "production":
    logger.warning(
        "Estas en produccion con SQLite. El historial se va a borrar en cada redespliegue. "
        "Agrega PostgreSQL y configura DATABASE_URL para que el agente recuerde a sus clientes."
    )

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

def ahora() -> datetime:
    """Hora actual en UTC, con zona horaria."""
    return datetime.now(timezone.utc)

class Base(DeclarativeBase):
    pass

class Mensaje(Base):
    """Un mensaje del historial de conversacion."""

    __tablename__ = "mensajes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telefono: Mapped[str] = mapped_column(String(50), index=True)
    role: Mapped[str] = mapped_column(String(20))  # "user" o "assistant"
    content: Mapped[str] = mapped_column(Text)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=ahora)

class EventoProcesado(Base):
    """
    Eventos de webhook que ya se atendieron.

    Los proveedores entregan "al menos una vez": el mismo evento puede llegar dos veces.
    Sin esta tabla, el cliente recibiria la misma respuesta repetida.
    """

    __tablename__ = "eventos_procesados"

    evento_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    creado_en: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=ahora, index=True)

async def inicializar_db():
    """Crea las tablas si no existen."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def marcar_evento_procesado(evento_id: str) -> bool:
    """
    Registra un evento. Retorna True si es nuevo, False si ya se habia procesado.

    La unicidad la garantiza la base de datos (clave primaria), no una consulta previa:
    asi dos webhooks que llegan al mismo tiempo no pasan los dos.
    """
    if not evento_id:
        return True  # sin id no podemos deduplicar: se procesa

    async with async_session() as session:
        session.add(EventoProcesado(evento_id=evento_id, creado_en=ahora()))
        try:
            await session.commit()
            return True
        except IntegrityError:
            await session.rollback()
            return False

async def liberar_evento(evento_id: str):
    """
    Borra la marca de un evento para que el reintento del proveedor SI se procese.

    Se usa cuando el mensaje se marco como procesado pero despues fallo el envio de la
    respuesta. Sin esto, el reintento se descartaria por duplicado y el cliente se
    quedaria sin respuesta para siempre.
    """
    if not evento_id:
        return
    async with async_session() as session:
        await session.execute(delete(EventoProcesado).where(EventoProcesado.evento_id == evento_id))
        await session.commit()

async def limpiar_eventos_viejos(dias: int = 7):
    """Borra los eventos de hace mas de N dias para que la tabla no crezca sin fin."""
    limite = ahora() - timedelta(days=dias)
    async with async_session() as session:
        resultado = await session.execute(
            delete(EventoProcesado).where(EventoProcesado.creado_en < limite)
        )
        await session.commit()
    if resultado.rowcount:
        logger.info(f"Se limpiaron {resultado.rowcount} eventos de mas de {dias} dias")

async def guardar_mensaje(telefono: str, role: str, content: str):
    """Guarda un mensaje en el historial de esa conversacion."""
    async with async_session() as session:
        session.add(Mensaje(telefono=telefono, role=role, content=content, timestamp=ahora()))
        await session.commit()

async def obtener_historial(telefono: str, limite: int = 20) -> list[dict]:
    """
    Devuelve los ultimos N mensajes de una conversacion, en orden cronologico.

    Se ordena por id y no por timestamp: dos mensajes guardados en el mismo instante
    tienen el mismo timestamp, y el orden entre ellos quedaria librado al azar.
    """
    async with async_session() as session:
        resultado = await session.execute(
            select(Mensaje)
            .where(Mensaje.telefono == telefono)
            .order_by(Mensaje.id.desc())
            .limit(limite)
        )
        mensajes = list(resultado.scalars().all())

    mensajes.reverse()  # vienen del mas nuevo al mas viejo: los damos vuelta

    # Las APIs de chat (Claude y Gemini) esperan que el historial empiece con un
    # mensaje del usuario.
    # Si por un error anterior quedo un "assistant" suelto al principio, lo sacamos.
    while mensajes and mensajes[0].role != "user":
        mensajes.pop(0)

    return [{"role": m.role, "content": m.content} for m in mensajes]

async def limpiar_historial(telefono: str):
    """Borra todo el historial de una conversacion."""
    async with async_session() as session:
        await session.execute(delete(Mensaje).where(Mensaje.telefono == telefono))
        await session.commit()


# ════════════════════════════════════════════════════════════
# SECCIÓN: Proveedor de WhatsApp — clase base (agent/providers/base.py)
# ════════════════════════════════════════════════════════════

@dataclass
class MensajeEntrante:
    """Mensaje normalizado: el mismo formato sin importar el proveedor."""

    telefono: str            # Numero del remitente, solo digitos, sin "+"
    texto: str               # Contenido del mensaje
    mensaje_id: str          # Id del mensaje en la plataforma
    es_propio: bool          # True si lo mando el agente (se ignora)
    contexto: dict = field(default_factory=dict)
    # "contexto" lleva lo que cada proveedor necesita para poder responder:
    #   evento_id       -> id unico del evento, para no procesar dos veces lo mismo
    #   conversation_id -> Zernio: en que conversacion hay que responder
    #   account_id      -> Zernio: que cuenta de WhatsApp recibio el mensaje

class ProveedorWhatsApp(ABC):
    """Interfaz que cada proveedor de WhatsApp debe implementar."""

    @abstractmethod
    async def parsear_webhook(self, request: Request) -> list[MensajeEntrante]:
        """Extrae y normaliza los mensajes del payload del webhook."""
        ...

    @abstractmethod
    async def enviar_mensaje(
        self, telefono: str, mensaje: str, contexto: dict | None = None
    ) -> bool:
        """Envia un mensaje de texto. Retorna True si salio bien."""
        ...

    async def verificar_firma(self, request: Request) -> bool:
        """
        Confirma que el webhook viene de verdad del proveedor.
        Por defecto acepta todo; cada proveedor lo implementa segun su esquema.
        """
        return True

    async def validar_webhook(self, request: Request) -> str | None:
        """Verificacion GET del webhook. Solo Meta la usa. Retorna la respuesta o None."""
        return None

    async def verificar_conexion(self) -> tuple[bool, str]:
        """Chequea que las credenciales sirvan. Retorna (ok, mensaje_legible)."""
        return True, "Este proveedor no expone un chequeo de conexion"


# ════════════════════════════════════════════════════════════
# SECCIÓN: Proveedor de WhatsApp — Zernio (agent/providers/zernio.py)
# ════════════════════════════════════════════════════════════

BASE_URL_POR_DEFECTO = "https://zernio.com/api/v1"

class ProveedorZernio(ProveedorWhatsApp):
    """Proveedor de WhatsApp usando Zernio."""

    def __init__(self):
        self.api_key = os.getenv("ZERNIO_API_KEY", "")
        self.webhook_secret = os.getenv("ZERNIO_WEBHOOK_SECRET", "")
        # Opcional: solo se usa para el chequeo de conexion al arrancar.
        # Para responder, el account_id sale del propio webhook.
        self.account_id = os.getenv("ZERNIO_ACCOUNT_ID", "")
        # Ojo con el "or": os.getenv(clave, default) solo usa el default si la clave NO
        # existe. Como el .env trae "ZERNIO_BASE_URL=" vacia, os.getenv devuelve "" y el
        # default nunca se aplicaria. El "or" cubre los dos casos.
        self.base_url = (os.getenv("ZERNIO_BASE_URL") or BASE_URL_POR_DEFECTO).rstrip("/")

        if not self.api_key:
            logger.warning("ZERNIO_API_KEY no esta configurada: el agente no va a poder responder")
        if not self.webhook_secret:
            logger.warning(
                "ZERNIO_WEBHOOK_SECRET no esta configurado: los webhooks NO se verifican. "
                "Sirve para probar, pero no lo dejes asi en produccion."
            )

    # ── Recibir ──────────────────────────────────────────────────────────

    async def verificar_firma(self, request: Request) -> bool:
        """Compara el header X-Zernio-Signature contra el HMAC-SHA256 del cuerpo crudo."""
        if not self.webhook_secret:
            return True  # modo pruebas, ya se advirtio al arrancar

        firma_recibida = request.headers.get("X-Zernio-Signature") or request.headers.get(
            "X-Late-Signature"
        )
        if not firma_recibida:
            logger.warning("Llego un webhook sin firma X-Zernio-Signature: rechazado")
            return False

        cuerpo = await request.body()
        firma_esperada = hmac.new(
            self.webhook_secret.encode("utf-8"), cuerpo, hashlib.sha256
        ).hexdigest()

        # compare_digest sobre str exige ASCII puro. Los headers HTTP pueden traer bytes
        # que Starlette decodifica como latin-1, y ahi tiraria TypeError: eso escaparia del
        # handler como un 500, y el proveedor reintentaria el evento siete veces.
        try:
            iguales = hmac.compare_digest(firma_esperada, firma_recibida)
        except TypeError:
            logger.warning("La firma del webhook trae caracteres invalidos: rechazado")
            return False

        if not iguales:
            logger.warning("Firma de webhook invalida: rechazado")
            return False
        return True

    async def parsear_webhook(self, request: Request) -> list[MensajeEntrante]:
        """Normaliza el evento message.received de Zernio."""
        payload = await request.json()

        evento = payload.get("event")
        if evento != "message.received":
            # message.sent, message.delivered, message.read, etc. no se contestan
            logger.debug(f"Evento ignorado: {evento}")
            return []

        mensaje = payload.get("message") or {}
        if mensaje.get("platform") != "whatsapp":
            return []

        remitente = mensaje.get("sender") or {}
        # phoneNumber viene en E.164 con "+". Desde abril de 2026 puede faltar
        # (los usuarios con username de WhatsApp no exponen su numero), asi que
        # caemos al identificador que Meta si garantiza.
        telefono = (remitente.get("phoneNumber") or "").lstrip("+")
        if not telefono:
            telefono = remitente.get("businessScopedUserId") or remitente.get("id") or ""

        cuenta = payload.get("account") or {}

        return [
            MensajeEntrante(
                telefono=telefono,
                texto=mensaje.get("text") or "",
                mensaje_id=mensaje.get("platformMessageId") or mensaje.get("id") or "",
                # Zernio marca la direccion: solo contestamos lo que entra
                es_propio=mensaje.get("direction") != "incoming",
                contexto={
                    "evento_id": payload.get("id", ""),
                    "conversation_id": mensaje.get("conversationId", ""),
                    "account_id": cuenta.get("id", ""),
                },
            )
        ]

    # ── Enviar ───────────────────────────────────────────────────────────

    async def enviar_mensaje(
        self, telefono: str, mensaje: str, contexto: dict | None = None
    ) -> bool:
        """Responde dentro de la conversacion que abrio el cliente."""
        contexto = contexto or {}
        conversation_id = contexto.get("conversation_id")
        account_id = contexto.get("account_id") or self.account_id

        if not self.api_key:
            logger.error("No se puede enviar: falta ZERNIO_API_KEY")
            return False
        if not conversation_id or not account_id:
            logger.error(
                "No se puede enviar: el mensaje no trae conversation_id o account_id"
            )
            return False

        url = f"{self.base_url}/inbox/conversations/{conversation_id}/messages"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        # Si el webhook se reintenta, esta clave evita que Zernio mande la respuesta dos veces
        evento_id = contexto.get("evento_id")
        if evento_id:
            headers["Idempotency-Key"] = f"agentkit-{evento_id}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as cliente:
                r = await cliente.post(
                    url,
                    json={"accountId": account_id, "message": mensaje},
                    headers=headers,
                )
        except httpx.HTTPError as e:
            logger.error(f"Error de red hablando con Zernio: {e}")
            return False

        if r.status_code == 200:
            return True

        # Zernio responde {"error": ..., "type": ..., "code": ...}.
        # Loguear los tres ahorra muchisimo tiempo de diagnostico.
        detalle = r.text[:500]
        try:
            cuerpo = r.json()
            detalle = (
                f"{cuerpo.get('error')} "
                f"(type={cuerpo.get('type')}, code={cuerpo.get('code')})"
            )
        except ValueError:
            pass
        logger.error(f"Zernio rechazo el envio [{r.status_code}]: {detalle}")
        return False

    # ── Diagnostico ──────────────────────────────────────────────────────

    async def verificar_conexion(self) -> tuple[bool, str]:
        """
        Pregunta a Zernio el estado real del numero contra Meta.
        Es la via soportada para saber si el canal esta vivo.
        """
        if not self.api_key:
            return False, "Falta ZERNIO_API_KEY"
        if not self.account_id:
            return True, "ZERNIO_ACCOUNT_ID no configurado: se omite el chequeo del numero"

        try:
            async with httpx.AsyncClient(timeout=15.0) as cliente:
                r = await cliente.get(
                    f"{self.base_url}/whatsapp/number-info",
                    params={"accountId": self.account_id},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
        except httpx.HTTPError as e:
            return False, f"No se pudo contactar a Zernio: {e}"

        if r.status_code != 200:
            return False, f"Zernio respondio {r.status_code}: {r.text[:200]}"

        telefono = r.json().get("phone") or {}
        return True, (
            f"Numero {telefono.get('display_phone_number', '?')} conectado "
            f"(calidad: {telefono.get('quality_rating', '?')})"
        )


# ════════════════════════════════════════════════════════════
# SECCIÓN: Fábrica de proveedores (agent/providers/__init__.py)
# ════════════════════════════════════════════════════════════

PROVEEDORES_SOPORTADOS = ("zernio",)


def obtener_proveedor() -> ProveedorWhatsApp:
    """
    Retorna el proveedor configurado en .env.

    Ojo: esto NO se ejecuta al importar el modulo. Si la configuracion esta
    mal, el servidor igual tiene que arrancar y contarlo en el health check,
    en vez de morirse en el import y dejar a Railway reiniciando a ciegas.

    En esta version de un solo archivo solo viene incluido el adaptador de
    Zernio, que es el proveedor que usa este agente (ver business.yaml).
    """
    proveedor = os.getenv("WHATSAPP_PROVIDER", "").strip().lower()

    if not proveedor:
        raise ValueError(
            "WHATSAPP_PROVIDER no esta configurado en el .env. "
            f"Valores validos: {' | '.join(PROVEEDORES_SOPORTADOS)}"
        )

    if proveedor == "zernio":
        return ProveedorZernio()

    raise ValueError(
        f"Proveedor no soportado en esta version de un solo archivo: '{proveedor}'. "
        f"Valores validos: {' | '.join(PROVEEDORES_SOPORTADOS)}"
    )


# ════════════════════════════════════════════════════════════
# SECCIÓN: Cerebro del agente — IA (agent/brain.py)
# ════════════════════════════════════════════════════════════

# ── Seleccion de proveedor ─────────────────────────────────
# Se cambia desde .env, sin tocar el codigo. Si la variable no esta, se usa
# "anthropic" para conservar el comportamiento original del plano.
PROVEEDOR_IA = (os.getenv("AI_PROVIDER") or "anthropic").strip().lower()

# ── Anthropic ──────────────────────────────────────────────
# El cliente se crea solo si el proveedor elegido es Anthropic, y con red de
# seguridad: si la clave no esta configurada, el SDK revienta al construirse y
# dejaria al agente sin arrancar. Con el error guardado, el arranque sigue y el
# health check puede decir exactamente que falta.
client = None
if PROVEEDOR_IA == "anthropic":
    try:
        client = AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    except Exception as e:  # noqa: BLE001 — falta la clave o esta mal formada
        logger.error(f"No se pudo crear el cliente de Anthropic: {e}")
        client = None

# El modelo se cambia desde .env, sin tocar el codigo.
#   claude-opus-5-5   el mas capaz             $4 / $20 por millon de tokens
#   claude-sonnet-5-5 el balanceado (default)  $2 / $10
#   claude-haiku-5-5  el mas barato y rapido   $0.10 / $0.50
# El "or" y no el default de os.getenv: una variable declarada vacia en el .env
# devuelve "" y dejaria al agente sin modelo.
MODELO = os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-5-5"

# Es un bot de respuestas cortas: con esfuerzo bajo contesta mas rapido y mas barato.
# Dejalo vacio en el .env para no mandar el parametro.
ESFUERZO = os.getenv("ANTHROPIC_EFFORT", "low").strip()

# WhatsApp son mensajes cortos, pero este tope NO es solo la respuesta: en los modelos
# actuales el razonamiento interno tambien cuenta contra el. Con el margen justo, una
# pregunta que exija pensar un poco deja al agente sin espacio para contestar.
MAX_TOKENS = int(os.getenv("ANTHROPIC_MAX_TOKENS") or "4096")

# Los modelos mas viejos no aceptan output_config. Si la primera llamada falla por eso,
# se reintenta sin el parametro y se recuerda para las siguientes.
_soporta_esfuerzo = True

# ── Google Gemini ──────────────────────────────────────────
# Nivel gratis de AI Studio. Gemini expone un endpoint compatible con OpenAI:
# las peticiones son POST a <base>/chat/completions con la clave como Bearer.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or ""
MODELO_GEMINI = os.getenv("GEMINI_MODEL") or "gemini-2.5-flash"
GEMINI_BASE_URL = (
    os.getenv("GEMINI_BASE_URL") or "https://generativelanguage.googleapis.com/v1beta/openai/"
).rstrip("/")
GEMINI_MAX_TOKENS = int(os.getenv("GEMINI_MAX_TOKENS") or "4096")
# Generar una respuesta puede tardar bastante mas que el timeout de 5s que trae
# httpx por defecto; con ese tope todas las llamadas "fallarian" sin ser un error.
GEMINI_TIMEOUT = float(os.getenv("GEMINI_TIMEOUT") or "60")

def verificar_configuracion_ia() -> tuple[bool, str]:
    """
    Chequeo de credenciales del proveedor de IA, sin hacer ninguna llamada.

    Devuelve (ok, detalle) con el mismo estilo que el chequeo del proveedor de
    WhatsApp ("Falta ZERNIO_API_KEY"), para que el health check diga exactamente
    que variable falta en vez de un error generico.
    """
    if PROVEEDOR_IA == "gemini":
        if not GEMINI_API_KEY:
            return False, "Falta GEMINI_API_KEY"
        return True, f"Gemini ({MODELO_GEMINI})"
    if PROVEEDOR_IA == "anthropic":
        if not os.getenv("ANTHROPIC_API_KEY"):
            return False, "Falta ANTHROPIC_API_KEY"
        return True, f"Claude ({MODELO})"
    return False, f"AI_PROVIDER no valido: {PROVEEDOR_IA!r}. Valores validos: gemini | anthropic"

def cargar_config_prompts() -> dict:
    """
    Lee la configuracion de prompts.

    Version de un solo archivo: si existe config/prompts.yaml se lee de ahi,
    igual que en el proyecto original; si no, se usa la copia incrustada en
    este mismo archivo (CONFIG_PROMPTS). Asi el agente funciona igual
    subiendo solo main.py, sin carpetas.
    """
    try:
        with open("config/prompts.yaml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return dict(CONFIG_PROMPTS)
def cargar_system_prompt() -> str:
    """El system prompt: quien es el agente y que sabe del negocio."""
    return cargar_config_prompts().get(
        "system_prompt", "Eres un asistente util. Responde siempre en espanol."
    )

def obtener_mensaje_error() -> str:
    """Que decirle al cliente cuando algo falla de nuestro lado."""
    return cargar_config_prompts().get(
        "error_message",
        "Lo siento, estoy teniendo problemas tecnicos. Por favor intenta de nuevo en unos minutos.",
    )

def obtener_mensaje_fallback() -> str:
    """Que decirle al cliente cuando no se entendio el mensaje."""
    return cargar_config_prompts().get(
        "fallback_message", "Disculpa, no entendi tu mensaje. Podrias reformularlo?"
    )

def _extraer_texto(respuesta) -> str:
    """
    Junta el texto de la respuesta de Claude.

    Ojo: NO se puede hacer respuesta.content[0].text. La respuesta es una lista de
    bloques y el primero no siempre es texto (los modelos que razonan devuelven
    primero un bloque de pensamiento). Hay que filtrar por tipo.
    """
    partes = [bloque.text for bloque in respuesta.content if bloque.type == "text"]
    return "\n".join(p for p in partes if p).strip()

def _es_error_de_esfuerzo(error: Exception) -> bool:
    """
    True solo si el modelo rechazo la llamada POR el parametro output_config/effort.

    Se exige que sea un 400 de peticion invalida y no cualquier error que mencione la
    palabra: un 529 de sobrecarga que la nombre de paso no debe apagar el parametro
    para todo el proceso.
    """
    if getattr(error, "status_code", None) != 400:
        return False
    texto = str(error).lower()
    return "output_config" in texto or "effort" in texto

async def _generar_con_anthropic(mensaje: str, historial: list[dict]) -> tuple[str, bool]:
    """Genera la respuesta con la API de Anthropic (ruta original del plano)."""
    global _soporta_esfuerzo

    if client is None:
        logger.error("Falta ANTHROPIC_API_KEY: el cliente de Anthropic no se pudo crear")
        return obtener_mensaje_error(), False

    mensajes = [{"role": m["role"], "content": m["content"]} for m in historial]
    mensajes.append({"role": "user", "content": mensaje})

    system_prompt = cargar_system_prompt()
    extras = {"output_config": {"effort": ESFUERZO}} if (_soporta_esfuerzo and ESFUERZO) else {}

    async def _llamar(parametros_extra: dict):
        return await client.messages.create(
            model=MODELO,
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=mensajes,
            **parametros_extra,
        )

    try:
        respuesta = await _llamar(extras)
    except Exception as e:  # noqa: BLE001
        if extras and _es_error_de_esfuerzo(e):
            logger.warning(
                f"El modelo {MODELO} no acepta output_config.effort; se reintenta sin ese parametro."
            )
            _soporta_esfuerzo = False
            try:
                respuesta = await _llamar({})
            except Exception as e2:  # noqa: BLE001
                logger.error(f"Error llamando a Claude: {e2}")
                return obtener_mensaje_error(), False
        else:
            logger.error(f"Error llamando a Claude: {e}")
            return obtener_mensaje_error(), False

    if getattr(respuesta, "stop_reason", None) == "max_tokens":
        logger.warning(
            f"La respuesta se corto por llegar al tope de {MAX_TOKENS} tokens. "
            "Si pasa seguido, sube ANTHROPIC_MAX_TOKENS o acorta el system prompt."
        )

    texto = _extraer_texto(respuesta)
    if not texto:
        logger.warning("Claude devolvio una respuesta sin texto")
        return obtener_mensaje_fallback(), False

    logger.info(
        f"Respuesta generada con {MODELO} "
        f"({respuesta.usage.input_tokens} in / {respuesta.usage.output_tokens} out)"
    )
    return texto, True

async def _llamar_gemini(mensajes: list[dict]) -> dict:
    """
    Una sola llamada al endpoint de Gemini compatible con OpenAI.

    Devuelve el JSON ya parseado. Si Google contesta con un error HTTP, lo
    convierte en excepcion con el cuerpo incluido: ahi viene el motivo exacto
    (p. ej. API_KEY_INVALID) y sin eso el fallo es imposible de diagnosticar.
    """
    async with httpx.AsyncClient(timeout=GEMINI_TIMEOUT) as http:
        respuesta = await http.post(
            f"{GEMINI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {GEMINI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MODELO_GEMINI,
                "messages": mensajes,
                "max_tokens": GEMINI_MAX_TOKENS,
            },
        )
    if respuesta.status_code != 200:
        raise RuntimeError(
            f"Gemini contesto HTTP {respuesta.status_code}: {respuesta.text[:500]}"
        )
    return respuesta.json()

async def _generar_con_gemini(mensaje: str, historial: list[dict]) -> tuple[str, bool]:
    """Genera la respuesta con Google Gemini (endpoint compatible con OpenAI)."""
    if not GEMINI_API_KEY:
        logger.error("Falta GEMINI_API_KEY: no se puede llamar a Gemini")
        return obtener_mensaje_error(), False

    # En el formato de OpenAI el system prompt va como primer mensaje, no como
    # parametro aparte como en Anthropic. El historial ya trae los roles
    # "user"/"assistant", que son los mismos que usa este formato.
    mensajes = [{"role": "system", "content": cargar_system_prompt()}]
    mensajes += [{"role": m["role"], "content": m["content"]} for m in historial]
    mensajes.append({"role": "user", "content": mensaje})

    try:
        datos = await _llamar_gemini(mensajes)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Error llamando a Gemini: {e}")
        return obtener_mensaje_error(), False

    try:
        eleccion = datos["choices"][0]
        texto = (eleccion["message"].get("content") or "").strip()
    except (KeyError, IndexError, TypeError, AttributeError):
        logger.warning(f"Gemini devolvio una respuesta con formato inesperado: {str(datos)[:500]}")
        return obtener_mensaje_fallback(), False

    if eleccion.get("finish_reason") == "length":
        logger.warning(
            f"La respuesta se corto por llegar al tope de {GEMINI_MAX_TOKENS} tokens. "
            "Si pasa seguido, sube GEMINI_MAX_TOKENS o acorta el system prompt."
        )

    if not texto:
        logger.warning("Gemini devolvio una respuesta sin texto")
        return obtener_mensaje_fallback(), False

    uso = datos.get("usage") or {}
    logger.info(
        f"Respuesta generada con {MODELO_GEMINI} "
        f"({uso.get('prompt_tokens', '?')} in / {uso.get('completion_tokens', '?')} out)"
    )
    return texto, True

async def generar_respuesta(mensaje: str, historial: list[dict]) -> tuple[str, bool]:
    """
    Genera una respuesta con el proveedor de IA elegido en AI_PROVIDER.

    Args:
        mensaje: el mensaje nuevo del cliente
        historial: los mensajes anteriores, [{"role": "user"|"assistant", "content": "..."}]

    Returns:
        (texto, es_respuesta_real)

        "es_respuesta_real" es False cuando lo que se devuelve es un aviso tecnico
        (error o fallback) y no una respuesta del agente. main.py lo usa para no
        guardar esos avisos en el historial: si se guardaran, quedarian contaminando
        el contexto de todos los mensajes siguientes.
    """
    if not mensaje or len(mensaje.strip()) < 2:
        return obtener_mensaje_fallback(), False

    if PROVEEDOR_IA == "gemini":
        return await _generar_con_gemini(mensaje, historial)
    if PROVEEDOR_IA == "anthropic":
        return await _generar_con_anthropic(mensaje, historial)

    logger.error(
        f"AI_PROVIDER no valido: {PROVEEDOR_IA!r}. Valores validos: gemini | anthropic"
    )
    return obtener_mensaje_error(), False


# ════════════════════════════════════════════════════════════
# SECCIÓN: Herramientas del negocio (agent/tools.py)
# ════════════════════════════════════════════════════════════

CARPETA_KNOWLEDGE = Path("knowledge")

def cargar_info_negocio() -> dict:
    """
    Carga la informacion del negocio: config/business.yaml si existe;
    si no, la copia incrustada en este archivo (CONFIG_NEGOCIO).
    """
    try:
        with open("config/business.yaml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return dict(CONFIG_NEGOCIO)
def obtener_horario() -> dict:
    """Retorna el horario de atencion del negocio."""
    info = cargar_info_negocio()
    return {
        "horario": info.get("negocio", {}).get("horario", "No disponible"),
        "esta_abierto": True,  # TODO: calcular segun la hora actual y el horario
    }

def buscar_en_knowledge(consulta: str) -> str:
    """
    Busca informacion en los archivos de /knowledge.
    Retorna los fragmentos que coinciden con la consulta.
    """
    if not CARPETA_KNOWLEDGE.is_dir():
        return "No hay archivos de conocimiento disponibles."

    resultados = []
    for ruta in sorted(CARPETA_KNOWLEDGE.iterdir()):
        if ruta.name.startswith(".") or not ruta.is_file():
            continue
        try:
            contenido = ruta.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue  # binarios y archivos ilegibles se saltean
        if consulta.lower() in contenido.lower():
            resultados.append(f"[{ruta.name}]: {contenido[:500]}")

    if resultados:
        return "\n---\n".join(resultados)
    return "No encontre informacion especifica sobre eso en mis archivos."

# ════════════════════════════════════════════════════════════
# Claude Code: agrega aqui las funciones especificas segun
# el caso de uso elegido por el usuario. Ejemplos:
#
# Si FAQ -> buscar_en_knowledge() ya esta listo arriba
#
# Si AGENDAR CITAS:
# def obtener_slots_disponibles(fecha: str) -> list[dict]: ...
# def reservar_cita(telefono, fecha, hora, servicio): ...
# def cancelar_cita(telefono, cita_id): ...
#
# Si TOMAR PEDIDOS:
# def agregar_al_carrito(telefono, producto, cantidad): ...
# def ver_carrito(telefono) -> list[dict]: ...
# def confirmar_pedido(telefono) -> dict: ...
#
# Si VENTAS / LEADS:
# def registrar_lead(telefono, nombre, interes): ...
# def calificar_lead(telefono) -> str: ...
# def escalar_a_vendedor(telefono, contexto): ...
#
# Si SOPORTE:
# def crear_ticket(telefono, problema) -> str: ...
# def consultar_ticket(ticket_id) -> dict: ...
# def escalar_ticket(ticket_id, razon): ...
# ════════════════════════════════════════════════════════════

# ── Ventas / Leads — Despega Digital ─────────────────────────
# Casos de uso elegidos en la entrevista: responder preguntas frecuentes y
# calificar y atender leads. Estas funciones quedan listas para usar; conectarlas
# al ciclo de tool use de Claude es un paso aparte (ver la nota del encabezado).

ARCHIVO_LEADS = Path(__file__).resolve().parent / "leads.jsonl"

# Mensaje oficial de paso a humano (del documento de entrenamiento del negocio)
MENSAJE_PASO_A_HUMANO = (
    "Te paso con nuestro equipo para que te atiendan personalmente. "
    "Te responden por este mismo WhatsApp lo antes posible. 🙌"
)

def registrar_lead(telefono: str, nombre: str = "", interes: str = "", datos: dict | None = None) -> dict:
    """
    Registra un lead interesado en los servicios de Despega Digital.

    Guarda una linea JSON en leads.jsonl (en la raiz del proyecto) con el telefono,
    el nombre del negocio/persona, su interes y los datos de calificacion que se
    hayan recopilado (giro, ciudad, si ya tiene web / Google / redes).
    """
    lead = {
        "telefono": telefono,
        "nombre": nombre,
        "interes": interes,
        "datos": datos or {},
        "registrado_en": datetime.now(timezone.utc).isoformat(),
    }
    try:
        with open(ARCHIVO_LEADS, "a", encoding="utf-8") as f:
            f.write(json.dumps(lead, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error(f"No se pudo guardar el lead de {telefono}: {e}")
        return {"ok": False, "error": str(e)}
    logger.info(f"Lead registrado: {telefono} — {nombre} — {interes}")
    return {"ok": True, "lead": lead}

def calificar_lead(
    telefono: str,
    tiene_web: bool = False,
    tiene_google: bool = False,
    tiene_redes: bool = False,
    vende_productos: bool = False,
) -> str:
    """
    Recomienda el paquete de Despega Digital segun la situacion del negocio.

    Sigue la regla del flujo de conversacion del negocio:
    vende productos -> Paquete 4 (Tienda en Linea); sin redes -> Paquete 3;
    sin perfil de Google -> Paquete 2; sin pagina web -> Paquete 1.
    """
    if vende_productos:
        paquete = "Paquete 4 · Tienda en Línea ($349.95)"
    elif not tiene_redes:
        paquete = "Paquete 3 · Despegue Total ($299.95)"
    elif not tiene_google:
        paquete = "Paquete 2 · Crecimiento ($199.95)"
    elif not tiene_web:
        paquete = "Paquete 1 · Esencial ($149.95)"
    else:
        paquete = "Ya tiene presencia completa: ofrecer planes de contenido o mantenimiento"
    logger.info(f"Lead {telefono} calificado -> {paquete}")
    return paquete

def escalar_a_vendedor(telefono: str, contexto: str = "") -> str:
    """
    Marca la conversacion para atencion humana y devuelve el mensaje de paso.

    Se usa en los casos de la seccion 6 del entrenamiento: el cliente pide una
    persona, hay un reclamo, ya pago y pregunta por su avance, pide un precio
    especial o un servicio fuera del catalogo.
    """
    logger.warning(f"Conversacion de {telefono} escalada a humano. Contexto: {contexto}")
    return MENSAJE_PASO_A_HUMANO


# ════════════════════════════════════════════════════════════
# SECCIÓN: Servidor FastAPI + webhook (agent/main.py)
# ════════════════════════════════════════════════════════════

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
# En desarrollo queremos el detalle de NUESTRO agente, no el de las librerias.
# Poner el nivel raiz en DEBUG llena la terminal de ruido de aiosqlite y httpx
# y hace imposible leer lo que hizo el agente.
logger.setLevel(logging.DEBUG if ENVIRONMENT == "development" else logging.INFO)

PORT = int(os.getenv("PORT", "8000"))

# Un candado por numero de telefono. En WhatsApp es normal que alguien mande "hola" y
# medio segundo despues la pregunta de verdad: sin esto los dos mensajes se procesarian
# en paralelo, los dos leerian el mismo historial y las escrituras quedarian intercaladas.
_candados: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

# Si la configuracion esta mal, guardamos el error y lo mostramos en el health check,
# en vez de reventar en el import y dejar a Railway reiniciando el contenedor a ciegas.
proveedor = None
error_configuracion: str | None = None
try:
    proveedor = obtener_proveedor()
except Exception as e:  # noqa: BLE001 — cualquier problema de configuracion
    error_configuracion = str(e)

# Resultado del chequeo de credenciales que se hace al arrancar. Se expone en el health
# check: que el servidor conteste no significa que el agente pueda responder por WhatsApp.
estado_proveedor: dict = {"ok": None, "detalle": "sin verificar"}

# Lo mismo para el proveedor de IA (AI_PROVIDER): sin su clave el servidor arranca
# igual, pero cada respuesta saldria como aviso tecnico. Mejor que se vea aqui.
estado_ia: dict = {"ok": None, "detalle": "sin verificar"}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Prepara la base de datos y chequea el proveedor al arrancar."""
    await inicializar_db()
    await limpiar_eventos_viejos()
    logger.info("Base de datos lista")
    logger.info(f"Servidor AgentKit escuchando en el puerto {PORT}")

    global estado_proveedor, estado_ia
    ok_ia, detalle_ia = verificar_configuracion_ia()
    estado_ia = {"ok": ok_ia, "detalle": detalle_ia}
    logger.info(f"Proveedor de IA: {'OK' if ok_ia else 'ERROR'} — {detalle_ia}")

    if proveedor is not None:
        logger.info(f"Proveedor de WhatsApp: {proveedor.__class__.__name__}")
        ok, detalle = await proveedor.verificar_conexion()
        estado_proveedor = {"ok": ok, "detalle": detalle}
        logger.info(f"Conexion con el proveedor: {'OK' if ok else 'ERROR'} — {detalle}")
    else:
        logger.error(f"Proveedor de WhatsApp NO configurado: {error_configuracion}")

    yield

app = FastAPI(title="AgentKit — WhatsApp AI Agent", version="2.0.0", lifespan=lifespan)

@app.get("/")
async def health_check():
    """Endpoint de salud para Railway y monitoreo."""
    if error_configuracion:
        return {"status": "error", "service": "agentkit", "detalle": error_configuracion}

    # Se responde 200 aunque las credenciales esten mal, para que Railway no marque el
    # deploy como caido y puedas leer el diagnostico. El detalle esta en el cuerpo.
    return {
        "status": "ok" if (estado_proveedor["ok"] and estado_ia["ok"]) else "degradado",
        "service": "agentkit",
        "proveedor": proveedor.__class__.__name__ if proveedor else None,
        "conexion": estado_proveedor,
        "ia": estado_ia,
    }

@app.get("/webhook")
async def webhook_verificacion(request: Request):
    """Verificacion GET del webhook. La pide Meta; para Zernio no hace nada."""
    if proveedor is None:
        raise HTTPException(status_code=503, detail=error_configuracion or "Proveedor no configurado")

    respuesta = await proveedor.validar_webhook(request)
    if respuesta is not None:
        return PlainTextResponse(respuesta)

    # Meta pide un 403 cuando manda hub.mode=subscribe y el verify_token no coincide.
    # Devolverle 200 le hace creer que la URL quedo verificada cuando no es cierto.
    if request.query_params.get("hub.mode") == "subscribe":
        raise HTTPException(status_code=403, detail="Verify token incorrecto")

    return {"status": "ok"}

@app.post("/webhook")
async def webhook_handler(request: Request, tareas: BackgroundTasks):
    """
    Recibe los mensajes de WhatsApp.

    Contesta 200 de inmediato y procesa el mensaje en segundo plano.

    Esto NO es un detalle de estilo. Los proveedores esperan un 2xx en unos 5 segundos y,
    si no lo reciben, reintentan el mismo evento hasta 7 veces. Como llamar a la IA tarda
    mas que eso, procesar antes de contestar hace que el cliente reciba la misma respuesta
    repetida. Por eso: responder primero, trabajar despues.
    """
    if proveedor is None:
        raise HTTPException(status_code=503, detail=error_configuracion or "Proveedor no configurado")

    if not await proveedor.verificar_firma(request):
        raise HTTPException(status_code=401, detail="Firma del webhook invalida")

    try:
        mensajes = await proveedor.parsear_webhook(request)
    except Exception as e:  # noqa: BLE001
        # Un payload raro no debe hacer que el proveedor reintente para siempre
        logger.error(f"No se pudo leer el webhook: {e}")
        return {"status": "ignorado"}

    encolados = 0
    for msg in mensajes:
        if msg.es_propio or not msg.texto.strip():
            continue

        # La entrega es "al menos una vez": el mismo evento puede llegar dos veces
        evento_id = msg.contexto.get("evento_id") or msg.mensaje_id
        if evento_id and not await marcar_evento_procesado(evento_id):
            logger.info(f"Evento repetido, se ignora: {evento_id}")
            continue

        logger.info(f"Mensaje de {msg.telefono}: {msg.texto}")
        tareas.add_task(procesar_mensaje, msg)
        encolados += 1

    return {"status": "ok", "encolados": encolados}

async def procesar_mensaje(msg: MensajeEntrante):
    """
    Genera la respuesta y la manda de vuelta. Corre fuera del ciclo del webhook.

    Se toma un candado por telefono: dos mensajes seguidos del mismo cliente se
    atienden en orden, no en paralelo, para que el historial no se mezcle.
    """
    evento_id = msg.contexto.get("evento_id") or msg.mensaje_id

    async with _candados[msg.telefono]:
        try:
            # El historial se lee ANTES de guardar el mensaje actual: brain.py agrega
            # el mensaje nuevo al final, y asi no queda duplicado.
            historial = await obtener_historial(msg.telefono)
            respuesta, es_respuesta_real = await generar_respuesta(msg.texto, historial)

            enviado = await proveedor.enviar_mensaje(msg.telefono, respuesta, msg.contexto)

            if not enviado:
                # El evento se marco como procesado ANTES de llegar hasta aca, para que dos
                # entregas simultaneas no se dupliquen. Si el envio fallo, hay que soltarlo:
                # si no, el reintento del proveedor se descartaria por duplicado y el cliente
                # se quedaria sin respuesta para siempre.
                logger.error(f"No se pudo enviar la respuesta a {msg.telefono}; se libera el evento")
                await liberar_evento(evento_id)
                return

            # Solo se guarda en el historial lo que de verdad es conversacion. Los avisos
            # tecnicos ("estoy teniendo problemas") no son un turno del agente: guardarlos
            # los deja contaminando el contexto de todos los mensajes que vengan despues.
            if es_respuesta_real:
                await guardar_mensaje(msg.telefono, "user", msg.texto)
                await guardar_mensaje(msg.telefono, "assistant", respuesta)

            logger.info(f"Respuesta enviada a {msg.telefono}: {respuesta}")

        except Exception as e:  # noqa: BLE001
            logger.exception(f"Error procesando el mensaje de {msg.telefono}: {e}")
            await liberar_evento(evento_id)
            try:
                await proveedor.enviar_mensaje(msg.telefono, obtener_mensaje_error(), msg.contexto)
            except Exception:  # noqa: BLE001
                logger.error("Tampoco se pudo avisarle al cliente del error")
