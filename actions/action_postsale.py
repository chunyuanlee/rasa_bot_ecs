from typing import Any, Dict, List, Text

from rasa_sdk import Action, Tracker
from rasa_sdk.events import SlotSet, ActionExecutionRejected
from rasa_sdk.executor import CollectingDispatcher

from uuid import uuid4
from .db import SessionLocal
from sqlalchemy import and_, or_
from datetime import datetime, timedelta
from .db_table_class import OrderDetail, Postsale, PostsaleReason

class AskOrderDetailIds(Action):
    """选择要进行售后的订单明细ID"""

    def name(self) -> str:
        return "action_ask_order_detail_ids"

    def run(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        events = []
        order_id = tracker.get_slot("order_id")
        # 查询该订单没有售后记录或售后已完成的订单明细
        with SessionLocal() as session:
            order_details = (
                session.query(OrderDetail)
                .filter(
                    OrderDetail.order_id == order_id,
                    or_(
                        ~OrderDetail.postsale.any(),  # 没有关联的售后记录
                        and_(
                            OrderDetail.postsale.any(),  # 有售后记录
                            OrderDetail.postsale.any(
                                Postsale.complete_time != None
                            ),  # 所有售后都已完成
                        ),
                    ),
                )
                .all()
            )
        if order_details:
            buttons = [
                {
                    "title": f"{order_detail.order_detail_id} - \
                    {order_detail.sku_name} × {order_detail.sku_count} - \
                    {order_detail.total_amount}-{order_detail.discount_amount}={order_detail.final_amount}",
                    "payload": f"/SetSlots(order_detail_ids={order_detail.order_detail_id})",
                }
                for order_detail in order_details
            ]
            if len(order_details) > 1:
                buttons.append(
                    {
                        "title": "全部",
                        "payload": f"/SetSlots(order_detail_ids={'&'.join([order_detail.order_detail_id for order_detail in order_details])})",
                    }
                )
            buttons.append(
                {"title": "取消", "payload": "/SetSlots(order_detail_ids=false)"}
            )
            dispatcher.utter_message(text="请选择要申请售后的订单明细", buttons=buttons)
        else:
            dispatcher.utter_message(text="没有可申请售后的订单明细")
            events.append(SlotSet("order_detail_ids", "false"))
            events.append(ActionExecutionRejected("action_listen"))
        return events

class AskPostsaleReason(Action):
    """选择售后类型"""

    def name(self) -> str:
        return "action_ask_postsale_type"

    def run(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        order_detail_ids = tracker.get_slot("order_detail_ids")
        order_detail_id = order_detail_ids.split("&")[0]
        with SessionLocal() as session:
            order_detail: OrderDetail = session.query(OrderDetail).get(order_detail_id)
        dispatcher.utter_message(
            text=f"- {order_detail.order_detail_id}\
                    \n- {order_detail.sku_name} × {order_detail.sku_count}\
                    \n- {order_detail.total_amount}-{order_detail.discount_amount}={order_detail.final_amount}\
                    \n  请选择售后类型",
            buttons=[
                {"title": "退款", "payload": "/SetSlots(postsale_type=退款)"},
                {"title": "退货", "payload": " /SetSlots(postsale_type=退货)"},
                {"title": "换货", "payload": " /SetSlots(postsale_type=换货)"},
                {"title": "取消", "payload": " /SetSlots(postsale_type=false)"},
            ],
        )
        return []

class AskPostsaleReason(Action):
    """选择售后原因"""

    def name(self) -> str:
        return "action_ask_postsale_reason"

    def run(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        order_detail_ids = tracker.get_slot("order_detail_ids")
        order_detail_id = order_detail_ids.split("&")[0]
        with SessionLocal() as session:
            order_detail: OrderDetail = session.query(OrderDetail).get(order_detail_id)
            product_category = order_detail.sku.product_category
            postsale_reasons = (
                session.query(PostsaleReason)
                .filter(
                    or_(
                        PostsaleReason.product_category.is_(None),
                        PostsaleReason.product_category
                        == product_category.product_category,
                    ),
                )
                .all()
            )
        buttons = [
            {
                "title": postsale_reason.postsale_reason,
                "payload": f"/SetSlots(postsale_reason={postsale_reason.postsale_reason})",
            }
            for postsale_reason in postsale_reasons
        ] + [
            {"title": "其他", "payload": " /SetSlots(postsale_reason=other)"},
            {"title": "取消", "payload": " /SetSlots(postsale_reason=false)"},
        ]
        dispatcher.utter_message(
            text=f"- {order_detail.order_detail_id}\
                    \n- {order_detail.sku_name} × {order_detail.sku_count}\
                    \n- {order_detail.total_amount}-{order_detail.discount_amount}={order_detail.final_amount}\
                    \n  请选择售后原因",
            buttons=buttons,
        )
        return []

class CommitPostsale(Action):
    """提交售后申请"""

    def name(self) -> str:
        return "action_commit_postsale"

    def run(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        order_detail_ids = tracker.get_slot("order_detail_ids")
        order_detail_id = order_detail_ids.split("&")[0]
        postsale_reason = tracker.get_slot("postsale_reason")
        postsale_type = tracker.get_slot("postsale_type")
        postsale = Postsale(
            postsale_id="pts" + uuid4().hex[:16],
            create_time=datetime.now(),
            order_detail_id=order_detail_id,
            postsale_reason=postsale_reason,
            postsale_status="审核中",
            receive_id=None,
            complete_time=None,
            refund_amount=None,
            postsale_type=postsale_type,
        )
        with SessionLocal() as session:
            order_detail: OrderDetail = session.query(OrderDetail).get(order_detail_id)
            postsale.receive_id = order_detail.order.receive_id
            postsale.refund_amount = (
                None if postsale_type == "换货" else order_detail.final_amount
            )
            # 如果原因是“不喜欢/不想要了”，且签收时间在7天内，且金额在100元以内，且售后类型为退换货，则跳过审核直接进行退换货
            if (
                postsale_reason == "不喜欢/不想要了"
                and datetime.now() - order_detail.order.delivered_time
                < timedelta(days=7)
                and order_detail.total_amount < 100
                and postsale_type in ["退货", "换货"]
            ):
                postsale.postsale_status = (
                    "退货中" if postsale_type == "退货" else "换退货"
                )
                dispatcher.utter_message(
                    text=f"满足7天退换货条件，系统将自动为您安排{postsale_type}"
                )
            # 如果原因是其他，从最新消息中获取原因
            else:
                if postsale_reason == "other":
                    postsale.postsale_reason = tracker.latest_message["text"]
                dispatcher.utter_message(
                    text=f"您的{postsale_type}申请已提交，审核结果将在48小时内通知您"
                )
            session.add(postsale)
            session.commit()
        return [SlotSet("order_detail_ids", "&".join(order_detail_ids.split("&")[1:]))]