import { defineUserConfig } from "vuepress";
import theme from "./theme.js";

export default defineUserConfig({
    base: "/TemplateKV/",

    dest: "./dist",

    locales: {
        "/": {
            lang: "en-US",
            title: "Docs Demo",
            description: "A docs demo for vuepress-theme-hope",
        },
        "/zh/": {
            lang: "zh-CN",
            title: "Database for HuanbingLu",
            description: "Always need a db",
        },
    },

    theme,

    shouldPrefetch: false,
});
