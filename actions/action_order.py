from datetime import datetime, timedelta
from typing import Any, Text, Dict, List
from uuid import uuid4

from rasa_sdk import Action, Tracker
from rasa_sdk.events import SlotSet, ActionExecutionRejected
from rasa_sdk.executor import CollectingDispatcher
from sqlalchemy import and_, or_, func
from sqlalchemy.orm import joinedload

from actions.db import SessionLocal
from actions.db_table_class import OrderInfo, Postsale, OrderStatus, ReceiveInfo, Region


class AskOrderId(Action):

    def name(self) -> Text:
        return "action_ask_order_id"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        # 整个函数的返回值
        events = []

        # 建立数据库连接，查询订单信息
        with SessionLocal() as session:
            order_infos = (
                session.query(OrderInfo)
                .join(OrderInfo.order_status_)  # 关联订单状态表
                .options(joinedload(OrderInfo.order_detail))  # 预加载 order_detail
                .filter(self.get_query_condition(tracker))
                .all()
            )
        order_nums = len(order_infos)
        if order_nums == 1:
            # 如果只有一个订单，询问是否查询此订单
            order_info = order_infos[0]
            message = [
                "查找到一个订单",
                f"[{order_info.order_status}]**订单ID**：{order_info.order_id}",
            ]
            # 找订单的详情
            for order_detail in order_info.order_detail:
                message.append(f"- {order_detail.sku_name} × {order_detail.sku_count}")

            # 返回订单结果给用户选择
            dispatcher.utter_message(
                text="\n".join(message),
                buttons=[
                    {
                        "title": "确认",
                        "payload": f"/SetSlots(order_id={order_info.order_id})",
                    },
                    {"title": "返回", "payload": "/SetSlots(order_id=false)"},
                ],
            )
        elif order_nums > 1:
            # 如果有多个订单，用户选择其中一个
            # 多个订单的buttons
            buttons = [
                {
                    # 拼接button内容，显示给用户的文本
                    "title": "\n".join(
                        [
                            f"[{order_info.order_status}]订单ID：{order_info.order_id}",
                        ]
                        +
                        [
                            f"- {order_detail.sku_name} × {order_detail.sku_count}"
                            for order_detail in order_info.order_detail
                        ]
                    ),
                    # 用户单击后做的动作，Rasa 接收到的实际消息内容
                    # 用户选择订单后，rasa设置 slot 为所选的订单ID
                    "payload": f"/SetSlots(order_id={order_info.order_id})",
                }
                for order_info in order_infos
            ]
            # 拼接上返回的按钮
            buttons.append({"title": "返回", "payload": "/SetSlots(order_id=false)"})
            # 发送订单列表给用户
            dispatcher.utter_message(text="请选择订单", buttons=buttons)
        else:
            # 没有订单
            dispatcher.utter_message(text="暂无订单")
            # 把slot：order_id 设置为false
            events.append(SlotSet("order_id", "false"))
            # 打断action_listen动作
            events.append(ActionExecutionRejected("action_listen"))

        return events

    def get_query_condition(self, tracker):
        """
        获取查询条件
        :param tracker:
        :return:
        """
        user_id = tracker.get_slot("user_id")
        goto = tracker.get_slot("goto")
        match goto:
            case "order_id_shipped":
                # 获取已发货的订单
                # 此处返回true，上边函数的filter参数为true，会返回所有已发货的订单
                return and_(
                    OrderInfo.user_id == user_id,
                    OrderInfo.order_status == "已发货",
                )
            # 后续有其他的查询条件和flow可以继续写case语句
            case "order_id_shipped_delivered":
                # 获取已发货和已签收的订单
                return and_(
                    OrderInfo.user_id == user_id,
                    OrderInfo.order_status.in_(["已发货", "已签收"]),
                )
            case "action_ask_order_id_before_completed_3_days":
                # 查询进行中，或3日内已完成的订单
                return and_(
                    OrderInfo.user_id == user_id,
                    OrderInfo.order_status != "已取消",
                    or_(
                        OrderInfo.order_status != "已完成",
                        OrderInfo.complete_time > datetime.now() - timedelta(days=3),
                    ),
                )
            case "action_ask_order_id_before_delivered":
                # 查询已签收之前状态的订单
                return and_(
                    OrderInfo.user_id == user_id,
                    OrderInfo.order_status != "已取消",
                    OrderStatus.status_code <= 320,
                )
            case "action_ask_order_id_before_shipped":
                # 查询已发货之前状态的订单
                return and_(
                    OrderInfo.user_id == user_id,
                    OrderInfo.order_status != "已取消",
                    OrderStatus.status_code <= 310,
                )
            case "action_ask_order_id_after_delivered":
                # 查询已签收、售后中、已完成的订单
                return and_(
                    OrderInfo.user_id == user_id,
                    OrderInfo.order_status != "已取消",
                    OrderStatus.status_code >= 330,
                )


class GetOrderDetail(Action):
    """获取订单详情"""

    def name(self) -> str:
        return "action_get_order_detail"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        order_id = tracker.get_slot("order_id")
        # 查询订单
        with SessionLocal() as session:
            order_info = (
                session.query(OrderInfo)
                .options(joinedload(OrderInfo.order_detail))
                .options(joinedload(OrderInfo.logistics))
                .options(joinedload(OrderInfo.receive))
                .options(joinedload(OrderInfo.order_status_))
                .filter_by(order_id=order_id)
                .first()
            )
        # 拼接订单信息
        message = [f"- [{order_info.order_status}]**订单ID**：{order_info.order_id}"]
        for k, v in {
            "创建时间": order_info.create_time,
            "支付时间": order_info.payment_time,
            "签收时间": order_info.delivered_time,
            "完成时间": order_info.complete_time,
        }.items():
            if v:
                message.append(f"  - {k}：{v}")
        # 拼接订单明细信息
        message.append("- **订单明细**：")
        total_total_amount = 0.0
        total_discount_amount = 0.0
        total_final_amount = 0.0
        for order_detail in order_info.order_detail:
            message.append(
                f"  - {order_detail.sku_name} × {order_detail.sku_count} | \
                {order_detail.total_amount}-{order_detail.discount_amount}={order_detail.final_amount}"
            )
            total_total_amount += float(order_detail.total_amount)
            total_discount_amount += float(order_detail.discount_amount)
            total_final_amount += float(order_detail.final_amount)
        message.append(
            f"  - **合计**：{total_total_amount}-{total_discount_amount}={total_final_amount}"
        )
        # 拼接收货信息
        message.extend(
            [
                "- **收货信息**：",
                f"  - 收货人：{order_info.receive.receiver_name}",
                f"  - 联系电话：{order_info.receive.receiver_phone}",
                f"  - 收货地址：{order_info.receive.receive_province}\
                {order_info.receive.receive_city}\
                {order_info.receive.receive_district}\
                {order_info.receive.receive_street_address}",
            ]
        )
        # 拼接最近物流信息
        logistics = order_info.logistics
        if logistics:
            message.append("- **最近物流信息**：")
            message.append(f"  - {logistics[0].logistics_tracking.splitlines()[-1]}")
        dispatcher.utter_message(text="\n".join(message))
        if order_info.order_status_.status_code < 400:
            return []
        # 打印售后信息
        # 获取所有订单明细ID
        order_detail_ids = [
            order_detail.order_detail_id for order_detail in order_info.order_detail
        ]
        # 查询每个订单明细最新的售后信息
        with SessionLocal() as session:
            # 子查询：postsale 按 order_detail_id 分组，取最大的 postsale.create_time
            subquery = (
                session.query(
                    Postsale.order_detail_id,
                    func.max(Postsale.create_time).label("max_time"),
                )
                .filter(Postsale.order_detail_id.in_(order_detail_ids))
                .group_by(Postsale.order_detail_id)
                .subquery()
            )
            postsales = (
                session.query(Postsale)
                .join(
                    subquery,
                    (Postsale.order_detail_id == subquery.c.order_detail_id)
                    & (Postsale.create_time == subquery.c.max_time),
                )
                .options(joinedload(Postsale.order_detail))
                .options(joinedload(Postsale.logistics))
                .all()
            )
        if not postsales:
            return []
        # 拼接售后信息
        for postsale in postsales:
            message = [
                f"- [{postsale.postsale_status}]**售后ID**：{postsale.postsale_id}"
            ]
            message.append("- **订单明细**：")
            message.append(
                f"  -{postsale.order_detail.sku_name} × {postsale.order_detail.sku_count}"
            )
            message.append(f"- **退款金额**：{postsale.refund_amount}")
            # 获取最新的物流信息
            if postsale.logistics:
                postsale.logistics = sorted(
                    postsale.logistics, key=lambda x: x.create_time, reverse=True
                )
                message.append("- **最近物流信息**：")
                message.append(
                    f"  - {postsale.logistics[0].logistics_tracking.splitlines()[-1]}"
                )
            dispatcher.utter_message(text="\n".join(message))
        return []


class AskReceiveId(Action):
    """重新选择现有的收货信息，或修改并新建收货信息"""

    def name(self) -> str:
        return "action_ask_receive_id"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        user_id = tracker.get_slot("user_id")
        order_id = tracker.get_slot("order_id")
        # 查询用户现有收货信息
        with SessionLocal() as session:
            receive_infos = session.query(ReceiveInfo).filter_by(user_id=user_id).all()
            current_receive_info = (
                session.query(OrderInfo).filter_by(order_id=order_id).first().receive
            )
        buttons = []
        for receive_info in receive_infos:
            buttons.append(
                {
                    "title": f"收货人姓名：{receive_info.receiver_name} - \
                    联系电话：{receive_info.receiver_phone} - \
                    收货地址：{receive_info.receive_province}\
                    {receive_info.receive_city}\
                    {receive_info.receive_district}\
                    {receive_info.receive_street_address}",
                    "payload": f"/SetSlots(receive_id={receive_info.receive_id})",
                }
            )
        buttons.extend(
            [
                {
                    "title": "修改并新建收货信息",
                    "payload": f"/SetSlots(receive_id=modify)",
                },
                {"title": "取消", "payload": f"/SetSlots(receive_id=false)"},
            ]
        )
        dispatcher.utter_message(
            text="请选择现有的收货信息，或修改并新建收货信息", buttons=buttons
        )
        return [
            SlotSet("receiver_name", current_receive_info.receiver_name),
            SlotSet("receiver_phone", current_receive_info.receiver_phone),
            SlotSet("receive_province", current_receive_info.receive_province),
            SlotSet("receive_city", current_receive_info.receive_city),
            SlotSet("receive_district", current_receive_info.receive_district),
            SlotSet(
                "receive_street_address", current_receive_info.receive_street_address
            ),
        ]


class AskReceiveProvince(Action):
    """询问收货省"""

    def name(self) -> str:
        return "action_ask_receive_province"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        with SessionLocal() as session:
            provinces = session.query(Region.province).distinct().all()
        buttons = [
            {
                "title": province[0],
                "payload": f"/SetSlots(receive_province={province[0]})",
            }
            for province in provinces
        ]
        dispatcher.utter_message(text="请选择省份", buttons=buttons)
        return []


class AskReceiveCity(Action):
    """询问收货市"""

    def name(self) -> str:
        return "action_ask_receive_city"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        receive_province = tracker.get_slot("receive_province")
        with SessionLocal() as session:
            cities = (
                session.query(Region.city)
                .filter(Region.province == receive_province)
                .distinct()
                .all()
            )
        buttons = [
            {"title": city[0], "payload": f"/SetSlots(receive_city={city[0]})"}
            for city in cities
        ]
        dispatcher.utter_message(text="请选择城市", buttons=buttons)
        return []


class AskReceiveDistrict(Action):
    """询问收货区"""

    def name(self) -> str:
        return "action_ask_receive_district"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        receive_city = tracker.get_slot("receive_city")
        with SessionLocal() as session:
            districts = (
                session.query(Region.district)
                .filter(Region.city == receive_city)
                .distinct()
                .all()
            )
        buttons = [
            {
                "title": district[0],
                "payload": f"/SetSlots(receive_district={district[0]})",
            }
            for district in districts
        ]
        dispatcher.utter_message(text="请选择城区", buttons=buttons)
        return []


class AskSetReceiveInfo(Action):
    """设置收货信息"""

    def name(self) -> str:
        return "action_ask_set_receive_info"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        receive_id = tracker.get_slot("receive_id")
        set_receive_info = tracker.get_slot("set_receive_info")
        # 获取收货信息
        if receive_id in ("modify", "modified"):
            receive_info = ReceiveInfo(
                receive_id="rec" + uuid4().hex[:16],
                user_id=tracker.get_slot("user_id"),
                receiver_name=tracker.get_slot("receiver_name"),
                receiver_phone=tracker.get_slot("receiver_phone"),
                receive_province=tracker.get_slot("receive_province"),
                receive_city=tracker.get_slot("receive_city"),
                receive_district=tracker.get_slot("receive_district"),
                receive_street_address=tracker.get_slot("receive_street_address"),
            )
        else:
            with SessionLocal() as session:
                receive_info = (
                    session.query(ReceiveInfo).filter_by(receive_id=receive_id).first()
                )
        # 如果确认修改，进行修改
        if set_receive_info:
            order_id = tracker.get_slot("order_id")
            with SessionLocal() as session:
                order_info = (
                    session.query(OrderInfo).filter_by(order_id=order_id).first()
                )
                # 如果没有使用已有的收货信息，向数据库中添加新的收货信息
                if receive_id in ("modify", "modified"):
                    # 查询收货信息是否已存在
                    old_receive_info = (
                        session.query(ReceiveInfo)
                        .filter(
                            ReceiveInfo.user_id == receive_info.user_id,
                            ReceiveInfo.receiver_name == receive_info.receiver_name,
                            ReceiveInfo.receiver_phone == receive_info.receiver_phone,
                            ReceiveInfo.receive_province
                            == receive_info.receive_province,
                            ReceiveInfo.receive_city == receive_info.receive_city,
                            ReceiveInfo.receive_district
                            == receive_info.receive_district,
                            ReceiveInfo.receive_street_address
                            == receive_info.receive_street_address,
                        )
                        .first()
                    )
                    if old_receive_info:
                        receive_info = old_receive_info
                        dispatcher.utter_message(
                            text="此收货信息已存在，将不再重复添加"
                        )
                    else:
                        session.add(receive_info)
                        session.flush()
                order_info.receive_id = receive_info.receive_id
                session.commit()
            dispatcher.utter_message(text="订单收货信息已修改")
        # 展示收货信息，询问是否确认修改
        else:
            message = [
                f"- 收货人姓名：{receive_info.receiver_name}",
                f"- 联系电话：{receive_info.receiver_phone}",
                f"- 收货省份：{receive_info.receive_province}",
                f"- 收货城市：{receive_info.receive_city}",
                f"- 收货城区：{receive_info.receive_district}",
                f"- 收货地址：{receive_info.receive_street_address}",
            ]
            dispatcher.utter_message(text="\n".join(message))
            dispatcher.utter_message(
                text="是否确认修改？",
                buttons=[
                    {"title": "确认", "payload": "/SetSlots(set_receive_info=true)"},
                    {"title": "取消", "payload": "/SetSlots(set_receive_info=false)"},
                ],
            )
        return []


class CancelOrder(Action):
    """取消订单"""

    def name(self) -> str:
        return "action_cancel_order"

    def run(
            self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[str, Any]
    ) -> List[Dict[Text, Any]]:
        order_id = tracker.get_slot("order_id")
        with SessionLocal() as session:
            order_info = session.query(OrderInfo).filter_by(order_id=order_id).first()
            old_order_status = order_info.order_status
            order_info.order_status = "已取消"
            order_info.complete_time = datetime.now()
            session.commit()
        message = "订单已取消"
        if old_order_status == "待发货":
            message += "，退款金额将在24小时内返还您的账户"
        dispatcher.utter_message(text=message)
        return []
