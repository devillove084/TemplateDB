import { defineUserConfig } from "vuepress";
import theme from "./theme.js";

export default defineUserConfig({
    base: "/TemplateKV/",

    dest: "./dist",

    locales: {
        "/": {
            lang: "en-US",
            title: "TemplateKV",
            description: "A dynamically split or combined cloud native database by workload.",
        },
        "/zh/": {
            lang: "zh-CN",
            title: "TemplateKV",
            description: "根据workload动态伸缩的云原生数据库",
        },
    },

    theme,

    shouldPrefetch: false,
});
