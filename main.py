from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional
import threading
import subprocess
import time
import os
import asyncio
import math

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import PoseArray, Pose
from px4_msgs.msg import (
    OffboardControlMode,
    TrajectorySetpoint,
    VehicleCommand,
    VehicleLocalPosition,
    VehicleStatus,
    BatteryStatus,
)

from mavsdk import System
from mavsdk.mission import MissionItem, MissionPlan
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy

app = FastAPI()

missions: Dict[int, dict] = {}
mission_paths: Dict[int, list] = {}
next_mission_id = 1

bridge_node = None
system_processes = []
ANDROID_QGC_IP = "172.20.10.3"

# Real drone specs — populated at startup from PX4
DRONE_SPEED_M_S    = 3.0
DRONE_POWER_DRAW_W = 7.5

# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────────────────────

class MissionCreate(BaseModel):
    start_x: float
    start_y: float
    end_x: float
    end_y: float


class SubMissionItem(BaseModel):
    start_x: float
    start_y: float
    end_x: float
    end_y: float


class SplitMissionRequest(BaseModel):
    main_start_x: float
    main_start_y: float
    main_end_x: float
    main_end_y: float
    sub_missions: List[SubMissionItem]


class StartMissionRequest(BaseModel):
    start_x: float
    start_y: float
    end_x: float
    end_y: float


# ─────────────────────────────────────────────────────────────────────────────
# ROS / BRIDGE
# ─────────────────────────────────────────────────────────────────────────────

class MissionBridge(Node):
    def __init__(self):
        super().__init__("mission_bridge_api")
        self._initial_battery_pct = 100.0

        self.current_position = {
            "x": 0.0,
            "y": 0.0,
            "z": 0.0,
            "available": False,
        }

        self.publisher = self.create_publisher(
            PoseArray,
            "/seeding_area",
            10,
        )

        qos_profile = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )

        self.position_subscriber = self.create_subscription(
            VehicleLocalPosition,
            "/fmu/out/vehicle_local_position_v1",
            self.position_callback,
            qos_profile,
        )

        self.battery_subscriber = self.create_subscription(
            BatteryStatus,
            "/fmu/out/battery_status_v1",
            self.battery_callback,
            qos_profile,
        )

        self.vehicle_status_subscriber = self.create_subscription(
            VehicleStatus,
            "/fmu/out/vehicle_status_v1",
            self.vehicle_status_callback,
            qos_profile,
        )

        self.power_draw_w        = 0.0
        self.energy_consumed_wh  = 0.0
        self._last_battery_time  = None
        self._tracking_energy    = False
        self.is_landed           = True
        self._mission_ended      = False
        self._final_energy_wh    = 0.0
        self._final_duration_min = 0
        self._active_mission_id  = None
        self.battery_remaining_pct = 100.0
        self._max_altitude         = 0.0
        self._altitude_sum         = 0.0
        self._altitude_count       = 0
        self._avg_altitude         = 0.0
        self._flight_start_time  = None  # set on arm, not on API call

    def publish_mission(self, start_x, start_y, end_x, end_y):
        msg = PoseArray()

        p1 = Pose()
        p1.position.x = float(start_x)
        p1.position.y = float(start_y)

        p2 = Pose()
        p2.position.x = float(end_x)
        p2.position.y = float(end_y)

        msg.poses = [p1, p2]
        self.publisher.publish(msg)

        self.get_logger().info(
            f"Published mission: start=({start_x}, {start_y}), end=({end_x}, {end_y})"
        )

    def position_callback(self, msg):
        self.current_position = {
            "x": float(msg.x),
            "y": float(msg.y),
            "z": float(msg.z),
            "available": True,
        }
        # Track altitude when mission is flying (NED: z negative = above ground)
        if self._tracking_energy:
            alt = abs(float(msg.z))
            if alt > self._max_altitude:
                self._max_altitude = alt
            self._altitude_sum   += alt
            self._altitude_count += 1

    def vehicle_status_callback(self, msg):
        # arming_state: 1 = disarmed, 2 = armed
        just_armed = (
            self.is_landed == True and
            msg.arming_state == 2
        )

        just_landed = (
            self.is_landed == False and
            msg.arming_state == 1
        )

        self.is_landed = (msg.arming_state == 1)

        if just_armed and self._tracking_energy:
            self._flight_start_time = time.time()
            self.get_logger().info("Drone armed — flight timer started")

        if just_landed and self._tracking_energy:
            self.get_logger().info("Landing detected — stopping energy tracking")
            self.stop_energy_tracking()
            self._mission_ended = True

    def battery_callback(self, msg):
        now     = time.time()
        voltage = float(msg.voltage_v)
        current = abs(float(msg.current_a))  # abs() — PX4 discharge is negative

        self.power_draw_w = voltage * current

        self.battery_remaining_pct = float(getattr(msg, 'remaining', 1.0)) * 100

        if self._tracking_energy and self._last_battery_time is not None:
            dt_hours = (now - self._last_battery_time) / 3600.0
            self.energy_consumed_wh += self.power_draw_w * dt_hours

        self._last_battery_time = now

    def start_energy_tracking(self):
        self.energy_consumed_wh  = 0.0
        self._last_battery_time  = time.time()
        self._tracking_energy    = True
        self._mission_ended      = False
        self._final_energy_wh    = 0.0
        self._max_altitude         = 0.0
        self._altitude_sum         = 0.0
        self._altitude_count       = 0
        self._avg_altitude         = 0.0
        self.battery_remaining_pct = 100.0
        self._final_duration_min = 0.0
        self._flight_start_time  = None  # reset — will be set on arm
        self._initial_battery_pct = self.battery_remaining_pct
        self.get_logger().info("Energy tracking started")


    def stop_energy_tracking(self) -> float:
       self._tracking_energy = False
       self._final_energy_wh = self.energy_consumed_wh

    # Battery used = change from start to end (not absolute from 100%)
       self._battery_used_pct = max(
           0.0,
           self._initial_battery_pct - self.battery_remaining_pct
       )

       if self._flight_start_time is not None:
           elapsed_s = time.time() - self._flight_start_time
           self._final_duration_min = round(elapsed_s / 60, 2)
       else:
           self._final_duration_min = 0.0

       self._avg_altitude = (
           self._altitude_sum / self._altitude_count
           if self._altitude_count > 0 else 0.0
       )

       self.get_logger().info(
           f"Flight ended: {self.energy_consumed_wh:.2f} Wh, "
           f"{self._final_duration_min} min, "
           f"battery used: {self._battery_used_pct:.1f}%")
       return self.energy_consumed_wh
# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def get_cruise_speed() -> float:
    try:
        drone = System()
        await drone.connect(system_address="udp://:14540")

        async for state in drone.core.connection_state():
            if state.is_connected:
                break

        result = await drone.param.get_param_float("MPC_XY_CRUISE")
        return result.value
    except Exception as e:
        print(f"Could not read cruise speed from PX4: {e}")
        return 3.0


def init_ros():
    global bridge_node

    if not rclpy.ok():
        rclpy.init()

    if bridge_node is None:
        bridge_node = MissionBridge()
        thread = threading.Thread(
            target=rclpy.spin,
            args=(bridge_node,),
            daemon=True,
        )
        thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# PATH GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_coverage_path(start_x, start_y, end_x, end_y, row_spacing=5.0):
    min_x = min(start_x, end_x)
    max_x = max(start_x, end_x)
    min_y = min(start_y, end_y)
    max_y = max(start_y, end_y)

    path = []
    current_y = min_y
    left_to_right = True

    while current_y <= max_y:
        if left_to_right:
            path.append({"x": min_x, "y": current_y})
            path.append({"x": max_x, "y": current_y})
        else:
            path.append({"x": max_x, "y": current_y})
            path.append({"x": min_x, "y": current_y})

        current_y += row_spacing
        left_to_right = not left_to_right


    return path


def _calculate_path_distance(path: list) -> float:
    total = 0.0
    for i in range(len(path) - 1):
        dx = path[i + 1]["x"] - path[i]["x"]
        dy = path[i + 1]["y"] - path[i]["y"]
        total += math.sqrt(dx * dx + dy * dy)
    return total


def check_validity(main_start_x, main_start_y, main_end_x, main_end_y,
                   sub_start_x, sub_start_y, sub_end_x, sub_end_y) -> bool:
    main_min_x = min(main_start_x, main_end_x)
    main_max_x = max(main_start_x, main_end_x)
    main_min_y = min(main_start_y, main_end_y)
    main_max_y = max(main_start_y, main_end_y)

    sub_min_x = min(sub_start_x, sub_end_x)
    sub_max_x = max(sub_start_x, sub_end_x)
    sub_min_y = min(sub_start_y, sub_end_y)
    sub_max_y = max(sub_start_y, sub_end_y)

    return (sub_min_x >= main_min_x and sub_max_x <= main_max_x and
            sub_min_y >= main_min_y and sub_max_y <= main_max_y)


def local_xy_to_gps(home_lat, home_lon, x_east, y_north):
    earth_radius = 6378137.0

    d_lat = y_north / earth_radius
    d_lon = x_east / (earth_radius * math.cos(math.radians(home_lat)))

    lat = home_lat + math.degrees(d_lat)
    lon = home_lon + math.degrees(d_lon)

    return lat, lon


async def upload_path_to_px4(path):
    if not path:
        raise RuntimeError("Path is empty")

    drone = System()
    await drone.connect(system_address="udp://:14540")

    print("Waiting for PX4 connection...")
    async for state in drone.core.connection_state():
        if state.is_connected:
            print("PX4 connected")
            break

    print("Waiting for home/global position...")
    async for position in drone.telemetry.position():
        home_lat = position.latitude_deg
        home_lon = position.longitude_deg

        if home_lat != 0.0 and home_lon != 0.0:
            break

    mission_items = []

    for point in path:
        lat, lon = local_xy_to_gps(
            home_lat,
            home_lon,
            point["x"],
            point["y"],
        )

        mission_items.append(
            MissionItem(
                latitude_deg=lat,
                longitude_deg=lon,
                relative_altitude_m=2.0,
                speed_m_s=3.0,
                is_fly_through=True,
                gimbal_pitch_deg=float("nan"),
                gimbal_yaw_deg=float("nan"),
                camera_action=MissionItem.CameraAction.NONE,
                loiter_time_s=float("nan"),
                camera_photo_interval_s=float("nan"),
                acceptance_radius_m=1.0,
                yaw_deg=float("nan"),
                camera_photo_distance_m=float("nan"),
                vehicle_action=MissionItem.VehicleAction.NONE,
            )
        )

    mission_plan = MissionPlan(mission_items)

    print("Uploading mission to PX4/QGC...")
    await drone.mission.upload_mission(mission_plan)
    print("Mission uploaded to PX4/QGC")


# ─────────────────────────────────────────────────────────────────────────────
# SIMULATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def start_simulation_system():
    global system_processes

    if system_processes:
        return

    os.makedirs("/home/layan/drone_logs", exist_ok=True)

    commands = [
        ("microxrce.log",   "MicroXRCEAgent udp4 -p 8888"),
        ("px4.log",         "cd ~/PX4-Autopilot && make px4_sitl gazebo-classic_iris"),
        ("planner_server.log",
         "source /opt/ros/humble/setup.bash && "
         "source ~/px4_ros2_ws/install/setup.bash && "
         "ros2 run nav2_planner planner_server --ros-args "
         "--params-file ~/px4_ros2_ws/src/nav2_drone_planner/config/planner_server.yaml"),
        ("lifecycle.log",
         "source /opt/ros/humble/setup.bash && "
         "source ~/px4_ros2_ws/install/setup.bash && "
         "ros2 lifecycle set /planner_server configure && "
         "ros2 lifecycle set /planner_server activate"),
        ("simple_planner.log",
         "source /opt/ros/humble/setup.bash && "
         "source ~/px4_ros2_ws/install/setup.bash && "
         "ros2 run nav2_drone_planner simple_planner"),
    ]

    delays = [3, 12, 5, 3, 5]

    for index, (log_name, command) in enumerate(commands):
        log_path = f"/home/layan/drone_logs/{log_name}"
        with open(log_path, "w") as log_file:
            process = subprocess.Popen(
                ["bash", "-lc", command],
                stdout=log_file,
                stderr=log_file,
            )
        system_processes.append(process)
        time.sleep(delays[index])


def connect_qgc_android():
    command = (
        f"cd ~/PX4-Autopilot && "
        f"echo 'mavlink start -u 14552 -r 4000000 -t {ANDROID_QGC_IP} -o 14551' | "
        f"Tools/mavlink_shell.py udp:127.0.0.1:14540"
    )
    log_path = "/home/layan/drone_logs/qgc_android.log"
    with open(log_path, "w") as log_file:
        subprocess.Popen(["bash", "-lc", command], stdout=log_file, stderr=log_file)


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup_event():
    global DRONE_SPEED_M_S
    init_ros()
    try:
        DRONE_SPEED_M_S = asyncio.run(get_cruise_speed())
        print(f"Cruise speed from PX4: {DRONE_SPEED_M_S} m/s")
    except Exception:
        print(f"Using fallback cruise speed: {DRONE_SPEED_M_S} m/s")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/missions")
def create_mission(mission: MissionCreate):
    global next_mission_id

    mission_id = next_mission_id
    next_mission_id += 1

    missions[mission_id] = {
        "id":      mission_id,
        "start_x": mission.start_x,
        "start_y": mission.start_y,
        "end_x":   mission.end_x,
        "end_y":   mission.end_y,
        "status":  "saved",
    }

    mission_paths[mission_id] = generate_coverage_path(
        mission.start_x, mission.start_y,
        mission.end_x,   mission.end_y,
    )

    return {
        "message":    "Mission saved successfully",
        "mission_id": mission_id,
        "mission":    missions[mission_id],
        "path":       mission_paths[mission_id],
    }


@app.get("/missions")
def get_missions():
    return {"missions": list(missions.values())}


@app.get("/missions/{mission_id}")
def get_mission(mission_id: int):
    if mission_id not in missions:
        raise HTTPException(status_code=404, detail="Mission not found")
    return {
        "mission": missions[mission_id],
        "path":    mission_paths.get(mission_id, []),
    }


@app.get("/missions/{mission_id}/path")
def get_mission_path(mission_id: int):
    if mission_id not in missions:
        raise HTTPException(status_code=404, detail="Mission not found")
    return {
        "mission_id": mission_id,
        "path":       mission_paths.get(mission_id, []),
    }


@app.post("/missions/{mission_id}/split")
def split_mission(mission_id: int, request: SplitMissionRequest):
    if mission_id not in missions:
        missions[mission_id] = {
            "id":      mission_id,
            "start_x": request.main_start_x,
            "start_y": request.main_start_y,
            "end_x":   request.main_end_x,
            "end_y":   request.main_end_y,
            "status":  "split",
        }
        mission_paths[mission_id] = generate_coverage_path(
            request.main_start_x, request.main_start_y,
            request.main_end_x,   request.main_end_y,
        )
    else:
        missions[mission_id]["status"] = "split"

    sub_mission_results = []

    for i, sub in enumerate(request.sub_missions):

        if not check_validity(
            request.main_start_x, request.main_start_y,
            request.main_end_x,   request.main_end_y,
            sub.start_x, sub.start_y,
            sub.end_x,   sub.end_y,
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Sub-mission {i + 1} is outside the main mission boundary.",
            )

        sub_key = mission_id * 1000 + (i + 1)

        path = generate_coverage_path(
            sub.start_x, sub.start_y,
            sub.end_x,   sub.end_y,
        )

        distance_m = _calculate_path_distance(path)
        duration_s = distance_m / DRONE_SPEED_M_S
        energy_wh  = (duration_s / 3600) * DRONE_POWER_DRAW_W

        missions[sub_key] = {
            "id":                        sub_key,
            "parent_id":                 mission_id,
            "index":                     i + 1,
            "start_x":                   sub.start_x,
            "start_y":                   sub.start_y,
            "end_x":                     sub.end_x,
            "end_y":                     sub.end_y,
            "status":                    "saved",
            "total_distance":            round(distance_m, 2),
            "estimated_flight_duration": round(duration_s / 60, 2),
            "energy_consumption":        round(energy_wh, 2),
        }
        mission_paths[sub_key] = path

        sub_mission_results.append({
            "id":                        sub_key,
            "index":                     i + 1,
            "start_x":                   sub.start_x,
            "start_y":                   sub.start_y,
            "end_x":                     sub.end_x,
            "end_y":                     sub.end_y,
            "path":                      path,
            "total_distance":            round(distance_m, 2),
            "estimated_flight_duration": round(duration_s / 60, 2),
            "energy_consumption":        round(energy_wh, 2),
        })

    missions[mission_id]["sub_missions"] = [
        {"id": r["id"], "index": r["index"]} for r in sub_mission_results
    ]

    return {
        "message":      "Mission split successfully",
        "mission_id":   mission_id,
        "sub_missions": sub_mission_results,
    }


@app.post("/missions/{mission_id}/register")
def register_mission(mission_id: int, mission_data: StartMissionRequest):
    if mission_id not in missions:
        # Generate path and calculate distance so energy endpoint has it
        path       = generate_coverage_path(
            mission_data.start_x, mission_data.start_y,
            mission_data.end_x,   mission_data.end_y,
        )
        distance_m = _calculate_path_distance(path)

        missions[mission_id] = {
            "id":             mission_id,
            "start_x":        mission_data.start_x,
            "start_y":        mission_data.start_y,
            "end_x":          mission_data.end_x,
            "end_y":          mission_data.end_y,
            "status":         "registered",
            "total_distance": round(distance_m, 2),  # ← now available
        }
        mission_paths[mission_id] = path

    return {"message": "Mission registered", "mission_id": mission_id}


@app.get("/missions/{mission_id}/sub-missions")
def get_sub_missions(mission_id: int):
    sub_keys = [
        k for k, v in missions.items()
        if v.get("parent_id") == mission_id
    ]
    sub_keys.sort()

    sub_list = []
    for key in sub_keys:
        m = missions[key]
        sub_list.append({
            "id":                        m["id"],
            "index":                     m["index"],
            "start_x":                   m["start_x"],
            "start_y":                   m["start_y"],
            "end_x":                     m["end_x"],
            "end_y":                     m["end_y"],
            "total_distance":            m.get("total_distance"),
            "estimated_flight_duration": m.get("estimated_flight_duration"),
            "energy_consumption":        m.get("energy_consumption"),
        })

    return sub_list


@app.post("/missions/{mission_id}/start")
def start_mission(mission_id: int, mission_data: Optional[StartMissionRequest] = None):
    global bridge_node

    if mission_id not in missions:
        if mission_data is None:
            raise HTTPException(
                status_code=404,
                detail="Mission not found. Please provide mission coordinates.",
            )
        missions[mission_id] = {
            "id":      mission_id,
            "start_x": mission_data.start_x,
            "start_y": mission_data.start_y,
            "end_x":   mission_data.end_x,
            "end_y":   mission_data.end_y,
            "status":  "saved",
        }
        mission_paths[mission_id] = generate_coverage_path(
            mission_data.start_x, mission_data.start_y,
            mission_data.end_x,   mission_data.end_y,
        )

    if bridge_node is None:
        raise HTTPException(status_code=500, detail="ROS bridge not initialized")

    start_simulation_system()
    time.sleep(20)
    connect_qgc_android()
    time.sleep(3)

    mission = missions[mission_id]

    # Start tracking — flight timer begins on arm (inside vehicle_status_callback)
    bridge_node.start_energy_tracking()

    bridge_node.publish_mission(
        mission["start_x"],
        mission["start_y"],
        mission["end_x"],
        mission["end_y"],
    )

    mission["status"] = "started"

    return {
        "message":    "Simulation started and mission sent successfully",
        "mission_id": mission_id,
        "mission":    mission,
        "path":       mission_paths.get(mission_id, []),
    }


@app.get("/drone/battery")
def get_battery():
    if bridge_node is None:
        return {"power_w": 0.0, "energy_wh": 0.0, "available": False}
    return {
        "power_w":   round(bridge_node.power_draw_w, 2),
        "energy_wh": round(bridge_node.energy_consumed_wh, 2),
        "available": True,
    }


@app.get("/missions/{mission_id}/energy")
def get_mission_energy(mission_id: int):
    if bridge_node is None:
        raise HTTPException(status_code=500, detail="ROS bridge not initialized")

    # Force stop if still tracking but drone is on the ground
    if bridge_node._tracking_energy:
        z = bridge_node.current_position.get("z", 0.0)
        if abs(z) < 0.5:
            bridge_node.stop_energy_tracking()
            bridge_node._mission_ended = True
        else:
            return {
                "mission_id":         mission_id,
                "status":             "in_flight",
                "energy_consumed_wh": 0.0,
                "real_duration_min":  0,
                "message":            "Mission still in flight",
            }

    # Duration and energy now come from MissionBridge (arm→disarm)
    energy_wh         = bridge_node._final_energy_wh
    real_duration_min = bridge_node._final_duration_min

    distance_m      = missions.get(mission_id, {}).get("total_distance", 0) or 0
    real_duration_s = real_duration_min * 60
    avg_speed       = distance_m / real_duration_s if real_duration_s > 0 else 0

    if mission_id in missions:
        missions[mission_id]["real_energy_wh"]   = energy_wh
        missions[mission_id]["real_duration_min"] = real_duration_min

    return {
        "mission_id":         mission_id,
        "status":             "landed",
        "energy_consumed_wh": round(energy_wh, 2),
        "real_duration_min":  real_duration_min,
        "current_power_w":    round(bridge_node.power_draw_w, 2),
        "battery_used_pct":   round(100 - bridge_node.battery_remaining_pct, 1),
        "max_altitude_m":     round(bridge_node._max_altitude, 2),
        "avg_altitude_m":     round(bridge_node._avg_altitude, 2),
        "avg_speed_ms":       round(avg_speed, 2),
        "battery_used_pct":   round(bridge_node._battery_used_pct, 1),
    }


@app.get("/drone/position")
def get_drone_position():
    if bridge_node is None:
        return {"x": 0.0, "y": 0.0, "z": 0.0, "available": False}
    return bridge_node.current_position


@app.post("/system/stop")
def stop_system():
    global system_processes

    for process in system_processes:
        try:
            process.terminate()
        except Exception:
            pass

    system_processes = []
    return {"message": "System stopped"}
